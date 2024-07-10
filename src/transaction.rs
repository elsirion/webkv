use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use embed_doc_image::embed_doc_image;
use futures_locks::Mutex;
use itertools::{merge_join_by, EitherOrBoth};

use crate::snapshot::{LazySnapshotIndex, Snapshot};
use crate::storage::AtomicStorage;
use crate::{Key, KeyRef, Value};

/// Reference to a database supporting optimistic, snapshot isolated
/// transactions
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

struct DatabaseInner {
    db: AtomicStorage,
    snapshot_index: LazySnapshotIndex,
    commit_lock: Mutex<()>,
}

/// Allows reading and writing data based on a snapshot of the database. All
/// writes will either be applied atomically when calling
/// [`Transaction::commit`] succeeds or rolled back if it fails or the
/// transaction is dropped without calling `commit`.
///
/// ## Implementation
/// To achieve snapshot isolation, the transaction struct uses the following
/// components:
///   - **DB Snapshot**: A [`Snapshot`] is a reference to the DB state at which
///     the transaction was started. The actual snapshot data is managed by
///     [`LazySnapshotIndex`], from which the [`Snapshot`] was obtained. Reading
///     from the snapshot will always return the value that was present in the
///     DB at the time the transaction was started.
///   - **Written Key-Value Pairs**: a map (`changes`) containing the values to
///     all keys that have been written to during the transaction. Reading from
///     this map will return the value that was written during the transaction.
///   - **Read Keys**: a set (`read_keys`) containing all keys that have been
///     read during the transaction.
///
/// ### Reading from the transaction
/// When reading a key, the transaction will first check if the key has been
/// written to during the transaction by looking it up in `changes`. If it has,
/// the value from `changes` will be returned. If not, the value from the
/// snapshot will be returned. In both cases, the key will be added to
/// `read_keys`.
///
/// ![Diagram of transaction read operations][tx_read]
///
/// ### Writing to the transaction
/// When writing to a key, the key-value pair is added to the `changes` map,
/// overwriting any value already present under that key.
///
/// ![Diagram of transaction write operations][tx_write]
///
/// ### Committing the transaction
/// When committing the transaction, the following steps are taken atomically
/// (protected by a commit lock):
///   1. Check for read-write conflicts by checking that none of the keys in
///      `read_keys` have been written to in the DB since the transaction
///      started (this is done using the snapshot index)
///   2. Check for write-write conflicts by checking that none of the keys in
///      `changes` have been written to in the DB since the transaction started
///      (this is done using the snapshot index)
///   3. Add the keys in `changes` to the snapshot index, it is crucial that
///      this is done before writing the changes to the DB so that the snapshot
///      index can backup the old values
///   4. Write the changes to the DB atomically
///
/// Not that the below diagram only makes sense seen in context of
/// [`LazySnapshotIndex`] since a lot of the transaction functionality is
/// delegated to it. ![Diagram of transaction commit][tx_commit]
#[embed_doc_image("tx_read", "images/transaction_read.png")]
#[embed_doc_image("tx_write", "images/transaction_write.png")]
#[embed_doc_image("tx_commit", "images/transaction_commit.png")]
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

    /// Start a new transaction, snapshotting the current DB state
    pub async fn transaction(&self) -> Transaction {
        let _lock = self.inner.commit_lock.lock().await;
        let snapshot = self.inner.snapshot_index.snapshot().await;
        Transaction {
            db: self.inner.clone(),
            snapshot,
            changes: Default::default(),
            read_keys: Default::default(),
        }
    }
}

impl Transaction {
    pub async fn get(&mut self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
        self.read_keys.insert(key.to_vec());
        if let Some(value) = self.changes.get(key) {
            return Ok(value.clone());
        }
        self.snapshot.get(key).await
    }

    pub async fn find_by_prefix(&mut self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        let transaction_changes_prefix_result = self
            .changes
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix));

        let snapshot_prefix_result = self.snapshot.find_by_prefix(prefix).await?;

        // TODO: maybe deduplicate with snapshot impl and avoid allocations by returning
        // iterators in some cases
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

    pub async fn commit(self) -> anyhow::Result<()> {
        let _commit_guard = self.db.commit_lock.lock().await;

        if self.changes.is_empty() {
            // A read-only transaction doesn't need to commit anything and can't have
            // conflicts
            return Ok(());
        }

        if self.snapshot.check_conflicts(&self.read_keys).await {
            return Err(anyhow::anyhow!("Read-write conflict detected"));
        }

        if self
            .snapshot
            .check_conflicts(&self.changes.keys().cloned().collect())
            .await
        {
            return Err(anyhow::anyhow!("Write-write conflict detected"));
        }

        self.db
            .snapshot_index
            .add_generation(&self.changes.keys().cloned().collect::<Vec<_>>())
            .await
            .expect("might be in inconsistent state at this point"); //TODO: fix
        self.db
            .db
            .write_atomically(self.changes.into_iter().collect())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join_all;
    use rand::prelude::IteratorRandom;
    use rand::rngs::StdRng;
    use rand::{thread_rng, Rng, SeedableRng};

    use crate::Database;

    async fn increment_or_insert_key(db: &Database, key: &[u8]) -> anyhow::Result<()> {
        let mut transaction = db.transaction().await;

        let value = transaction
            .get(key)
            .await?
            .map(|value_bytes| u64::from_be_bytes(value_bytes.try_into().unwrap()))
            .unwrap_or_default();

        let new_value_bytes = (value + 1).to_be_bytes().to_vec();

        transaction.set(key.to_vec(), new_value_bytes);
        transaction.commit().await
    }

    async fn parallel_transactions() {
        const PARALLEL_TRANSACTIONS: usize = 100;
        let db = Database::new(Arc::new(crate::MemStorage::default()));

        let fail_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let tasks = (0..PARALLEL_TRANSACTIONS).map(|_| {
            let db_inner = db.clone();
            let fail_count = fail_count.clone();
            async move {
                while let Err(_) = increment_or_insert_key(&db_inner, b"counter").await {
                    fail_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        join_all(tasks).await;

        assert_eq!(
            db.transaction().await.get(b"counter").await.unwrap(),
            Some(PARALLEL_TRANSACTIONS.to_be_bytes().to_vec())
        );
    }

    async fn random_transaction(
        db: &Database,
        increment_keys: impl IntoIterator<Item = &[u8]>,
        read_keys: impl IntoIterator<Item = &[u8]>,
    ) -> anyhow::Result<()> {
        let mut transaction = db.transaction().await;

        for key in increment_keys {
            let value = transaction
                .get(key)
                .await?
                .map(|value_bytes| u64::from_be_bytes(value_bytes.try_into().unwrap()))
                .unwrap_or_default();

            let new_value_bytes = (value + 1).to_be_bytes().to_vec();

            transaction.set(key.to_vec(), new_value_bytes);
        }

        for key in read_keys {
            transaction.get(key).await?;
        }

        transaction.commit().await
    }

    async fn parallel_random_transactions() {
        const PARALLEL_WORKERS: usize = 10;
        const TRANSACTIONS_PER_WORKER: usize = 10;
        const TRANSACTION_DELAY_RANGE: std::ops::Range<Duration> =
            Duration::from_millis(10)..Duration::from_millis(100);
        const WRITES_PER_TRANSACTION: usize = 10;
        const READS_PER_TRANSACTION: usize = 10;
        const NUM_KEYS: usize = 50;

        let mut rng = thread_rng();
        let db = Database::new(Arc::new(crate::MemStorage::default()));
        let fail_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let possible_keys = (0..NUM_KEYS).map(|i| i.to_be_bytes()).collect::<Vec<_>>();

        let tasks = (0..PARALLEL_WORKERS).map(|_| {
            let db_inner = db.clone();
            let write_keys = possible_keys
                .iter()
                .choose_multiple(&mut rng, WRITES_PER_TRANSACTION);
            let read_keys = possible_keys
                .iter()
                .choose_multiple(&mut rng, READS_PER_TRANSACTION);
            let mut rng_inner = StdRng::from_rng(&mut rng).unwrap();
            let fail_count_inner = fail_count.clone();
            async move {
                for _ in 0..TRANSACTIONS_PER_WORKER {
                    tokio::time::sleep(rng_inner.gen_range(TRANSACTION_DELAY_RANGE.clone())).await;
                    while let Err(_) = random_transaction(
                        &db_inner,
                        read_keys.iter().map(|k| k.as_slice()),
                        write_keys.iter().map(|k| k.as_slice()),
                    )
                    .await
                    {
                        fail_count_inner.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tokio::time::sleep(rng_inner.gen_range(TRANSACTION_DELAY_RANGE.clone()))
                            .await;
                    }
                }
            }
        });

        join_all(tasks).await;

        assert_eq!(
            db.transaction()
                .await
                .find_by_prefix(&[])
                .await
                .unwrap()
                .into_iter()
                .map(|(_key, val)| { u64::from_be_bytes(val.try_into().unwrap()) as usize })
                .sum::<usize>(),
            PARALLEL_WORKERS * TRANSACTIONS_PER_WORKER * WRITES_PER_TRANSACTION
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_parallel_transactions_mt1() {
        parallel_transactions().await;
        parallel_random_transactions().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_parallel_transactions_st() {
        parallel_transactions().await;
        parallel_random_transactions().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_transactions_mt4() {
        parallel_transactions().await;
        parallel_random_transactions().await;
    }
}
