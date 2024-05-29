use crate::storage::AtomicStorage;
use crate::{Key, Value};
use futures::TryFutureExt;
use itertools::{merge_join_by, sorted, EitherOrBoth};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub type Generation = u64;

struct LazySnapshotIndex {
    inner: Arc<RwLock<LazySnapshotIndexInner>>,
}

struct LazySnapshotIndexInner {
    db: AtomicStorage,
    current_generation: Generation,
    active_generations: BTreeMap<Generation, ActiveGeneration>,
    /// For each key the generation maps to the previous value that was overwritten by that generation.
    snapshots: BTreeMap<Key, BTreeMap<Generation, Option<Value>>>,
}

#[derive(Debug)]
struct ActiveGeneration {
    references: usize,
    mutated_keys: HashSet<Key>,
}

/// Explicitly not cloneable
pub struct Snapshot {
    generation: Generation,
    inner: Arc<RwLock<LazySnapshotIndexInner>>,
}

impl LazySnapshotIndex {
    pub fn new(db: AtomicStorage) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LazySnapshotIndexInner {
                db,
                current_generation: 1,
                active_generations: BTreeMap::new(),
                snapshots: Default::default(),
            })),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut inner = self.inner.write().expect("Poisoned lock");
        let generation = inner.current_generation;
        let active_generations_entry =
            inner
                .active_generations
                .entry(generation)
                .or_insert_with(|| ActiveGeneration {
                    references: 0,
                    mutated_keys: HashSet::new(),
                });
        active_generations_entry.references += 1;

        Snapshot {
            generation,
            inner: self.inner.clone(),
        }
    }

    pub async fn remove_snapshot(&self, snapshot: Snapshot) {
        let mut inner = self.inner.write().expect("Poisoned lock");
        let active_generation = inner
            .active_generations
            .get_mut(&snapshot.generation)
            .expect("Generation not found");
        active_generation.references -= 1;

        // Now that we have removed a reference to the generation we can check if it can be removed. We only remove from oldest to newest since removing generations in the middle would require complicated checks if some of the cached keys held by it might still be needed.

        // Find the largest set of consecutive generations that can be removed starting from the oldest one
        let removable_generations = inner
            .active_generations
            .iter()
            .take_while(|(_, active_generation)| active_generation.references == 0)
            .map(|(generation, _)| *generation)
            .collect::<Vec<_>>();
        // TODO: We might be removing one generation less than we could, since snapshots will only ever read generations strictly larger than their own, but that seems ok for now.
        let max_removable_generation = removable_generations.last().copied().unwrap_or(0);

        // Collect all keys that we need to visit since they were mutated in the removable generations and thus hold value backups for older snapshots to read
        let keys_to_visit = removable_generations
            .iter()
            .flat_map(|generation| {
                inner
                    .active_generations
                    .remove(generation)
                    .expect("exists")
                    .mutated_keys
            })
            .collect::<Vec<_>>();

        for key in keys_to_visit {
            let snapshot_entry = inner.snapshots.get_mut(&key).expect("key not found");

            // Remove all value generations that are inside the removable range
            *snapshot_entry = snapshot_entry.split_off(&(max_removable_generation + 1));
            if snapshot_entry.is_empty() {
                inner.snapshots.remove(&key);
            }
        }
    }

    pub fn add_generation(&self, changes: &[Key]) -> anyhow::Result<()> {
        // FIXME: don't write if no transactions are active
        // TODO: maybe even filter out keys that are shadowed anyway

        let mut inner = self.inner.write().expect("Poisoned lock");
        inner.current_generation += 1;
        let current_generation = inner.current_generation;

        for key in changes {
            let maybe_value = inner.db.get(key)?;
            let snapshot_entry = inner
                .snapshots
                .entry(key.clone())
                .or_default();
            snapshot_entry.insert(current_generation, maybe_value);
        }

        Ok(())
    }
}

impl Snapshot {
    pub fn get(&self, key: &Key) -> anyhow::Result<Option<Value>> {
        let inner = self.inner.read().expect("Poisoned lock");
        inner
            .snapshots
            .get(key)
            .and_then(|snapshot| {
                snapshot
                    .range((self.generation + 1)..)
                    .next()
                    .map(|(_generation, value)| value.clone())
            })
            .map(Result::Ok)
            .unwrap_or_else(|| inner.db.get(key))
    }

    pub fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        let inner = self.inner.read().expect("Poisoned lock");
        let snapshot_prefix_result = inner
            .snapshots
            .range(prefix.to_vec()..)
            .flat_map(|(key, generations)| {
                generations
                    .range((self.generation + 1)..)
                    .next()
                    .map(|(_generation, value)| (key.clone(), value.clone()))
            })
            .collect::<Vec<_>>();

        let db_prefix_result = inner.db.find_by_prefix(prefix)?;

        // TODO: debug_assert is_sorted for both lists

        let result = merge_join_by(
            snapshot_prefix_result,
            db_prefix_result,
            |(key1, _), (key2, _)| key1.cmp(key2),
        )
            .filter_map(|either| match either {
                // Always use snapshot values …
                EitherOrBoth::Left((key, maybe_value)) => maybe_value.map(|value| (key, value)),
                // … but if there is no snapshot value, use the db value …
                EitherOrBoth::Right((key, value)) => Some((key, value)),
                // … and if there is both, use the snapshot value.
                EitherOrBoth::Both((key, maybe_value), _db) => maybe_value.map(|value| (key, value)),
            })
            .collect::<Vec<_>>();

        Ok(result)
    }
}
