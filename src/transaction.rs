use crate::snapshot::{LazySnapshotIndex, Snapshot};
use crate::storage::AtomicStorage;
use crate::{Key, KeyRef, Value};
use itertools::{merge_join_by, EitherOrBoth};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

pub struct Database {
    inner: Arc<DatabaseInner>,
}

struct DatabaseInner {
    db: AtomicStorage,
    snapshot_index: LazySnapshotIndex,
    commit_lock: Mutex<()>,
}

pub struct Transaction {
    db: Arc<DatabaseInner>,
    snapshot: Snapshot,
    changes: BTreeMap<Key, Option<Value>>,
    read_keys: HashSet<Key>,
}

impl Database {
    pub fn new(db: AtomicStorage) -> Self {
        let snapshot_index = LazySnapshotIndex::new(db.clone());
        Self {
            inner: Arc::new(DatabaseInner {
                db,
                snapshot_index,
                commit_lock: Mutex::new(()),
            }),
        }
    }

    pub fn transaction(&self) -> Transaction {
        let snapshot = self.inner.snapshot_index.snapshot();
        Transaction {
            db: self.inner.clone(),
            snapshot,
            changes: Default::default(),
            read_keys: Default::default(),
        }
    }
}

impl Transaction {
    pub fn get(&mut self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
        self.read_keys.insert(key.to_vec());
        if let Some(value) = self.changes.get(key) {
            return Ok(value.clone());
        }
        self.snapshot.get(key)
    }

    pub fn find_by_prefix(&mut self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        let transaction_changes_prefix_result = self
            .changes
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix));

        let snapshot_prefix_result = self.snapshot.find_by_prefix(prefix)?;

        // TODO: maybe deduplicate with snapshot impl and avoid allocations by returning iterators in some cases
        let result = merge_join_by(
            transaction_changes_prefix_result,
            snapshot_prefix_result,
            |(key1, _), (key2, _)| (*key1).cmp(key2),
        )
        .filter_map(|either| match either {
            EitherOrBoth::Left((key, maybe_value)) => {
                // TODO: restructure to avoid many inserts
                self.read_keys.insert(key.clone());
                maybe_value.clone().map(|value| (key.clone(), value))
            }
            EitherOrBoth::Right((key, value)) => {
                self.read_keys.insert(key.clone());
                Some((key, value))
            }
            EitherOrBoth::Both((key, maybe_value), _snapshot) => {
                self.read_keys.insert(key.clone());
                maybe_value.clone().map(|value| (key.clone(), value))
            }
        })
        .collect::<Vec<_>>();

        Ok(result)
    }

    pub fn set(&mut self, key: Key, value: Value) {
        self.changes.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Key) {
        self.changes.insert(key, None);
    }

    pub fn commit(self) -> anyhow::Result<()> {
        let _commit_guard = self.db.commit_lock.lock().expect("poisoned");

        if self.changes.is_empty() {
            // A read-only transaction doesn't need to commit anything and can't have conflicts
            return Ok(());
        }

        if self.snapshot.check_conflicts(&self.read_keys) {
            return Err(anyhow::anyhow!("Read-write conflict detected"));
        }

        if self
            .snapshot
            .check_conflicts(&self.changes.keys().cloned().collect())
        {
            return Err(anyhow::anyhow!("Write-write conflict detected"));
        }

        self.db
            .snapshot_index
            .add_generation(&self.changes.keys().cloned().collect::<Vec<_>>())
            .expect("might be in inconsistent state at this point"); //TODO: fix
        self.db
            .db
            .write_atomically(self.changes.into_iter().collect())
    }
}
