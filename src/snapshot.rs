use crate::storage::AtomicStorage;
use crate::{Key, KeyRef, Value};
use anyhow::anyhow;
use futures::TryFutureExt;
use itertools::{merge_join_by, sorted, EitherOrBoth};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};
use tracing::debug;

pub type Generation = u64;

pub struct LazySnapshotIndex {
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

    /// Has to be called before writing changes to the database to generate a sparse snapshot of the current DB state.
    pub fn add_generation(&self, changes: &[Key]) -> anyhow::Result<()> {
        // FIXME: don't write if no transactions are active
        // TODO: maybe even filter out keys that are shadowed anyway

        let mut inner = self.inner.write().expect("Poisoned lock");
        inner.current_generation += 1;
        let current_generation = inner.current_generation;

        for key in changes {
            let maybe_value = inner.db.get(key)?;
            let snapshot_entry = inner.snapshots.entry(key.clone()).or_default();
            snapshot_entry.insert(current_generation, maybe_value);
        }

        Ok(())
    }
}

impl LazySnapshotIndexInner {
    fn remove_snapshot(&mut self, snapshot: &Snapshot) {
        let active_generation = self
            .active_generations
            .get_mut(&snapshot.generation)
            .expect("Generation not found");
        active_generation.references -= 1;

        // Now that we have removed a reference to the generation we can check if it can be removed. We only remove from oldest to newest since removing generations in the middle would require complicated checks if some of the cached keys held by it might still be needed.

        // Find the largest set of consecutive generations that can be removed starting from the oldest one
        let removable_generations = self
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
                self.active_generations
                    .remove(generation)
                    .expect("exists")
                    .mutated_keys
            })
            .collect::<Vec<_>>();

        for key in keys_to_visit {
            let snapshot_entry = self.snapshots.get_mut(&key).expect("key not found");

            // Remove all value generations that are inside the removable range
            *snapshot_entry = snapshot_entry.split_off(&(max_removable_generation + 1));
            if snapshot_entry.is_empty() {
                self.snapshots.remove(&key);
            }
        }
    }
}

impl Debug for LazySnapshotIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read().expect("Poisoned lock");
        f.debug_struct("LazySnapshotIndex")
            .field("current_generation", &inner.current_generation)
            .field("active_generations", &inner.active_generations)
            .field("snapshots", &inner.snapshots)
            .finish()
    }
}

impl Snapshot {
    pub fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
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

    /// Check if any newer generations than the one the snapshot was created in have mutated the `keys` supplied.
    pub fn check_conflicts(&self, keys: &HashSet<Key>) -> bool {
        let inner = self.inner.read().expect("Poisoned lock");
        inner.active_generations.range((self.generation + 1)..).any(
            |(generation_idx, generation)| {
                let conflicts = generation
                    .mutated_keys
                    .intersection(keys)
                    .collect::<Vec<_>>();
                debug!(
                    "Conflict on keys: {:?} in generation {}",
                    conflicts, generation_idx
                );

                !conflicts.is_empty()
            },
        )
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.inner
            .write()
            .expect("Poisoned lock")
            .remove_snapshot(self);
    }
}

#[cfg(test)]
mod tests {
    use crate::snapshot::{LazySnapshotIndex, Snapshot};
    use crate::storage::memory::MemStorage;
    use crate::storage::IAtomicStorage;
    use std::sync::Arc;

    #[test]
    fn test_snapshot() {
        let db = Arc::new(MemStorage::default());
        let initial_db = vec![
            (b"k1".to_vec(), Some(b"value1".to_vec())),
            (b"k2".to_vec(), Some(b"value2".to_vec())),
            (b"k3".to_vec(), Some(b"value3".to_vec())),
            (b"a1".to_vec(), Some(b"other_value".to_vec())),
        ];
        db.write_atomically(initial_db).unwrap();

        let snapshot_index = LazySnapshotIndex::new(db.clone());
        assert_eq!(snapshot_index.inner.read().unwrap().current_generation, 1);

        fn assert_s0_reads_initial(s0: &Snapshot) {
            assert_eq!(s0.generation, 1);
            assert_eq!(s0.get(b"k1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(s0.find_by_prefix(b"k").unwrap().len(), 3);
        }
        let s0 = snapshot_index.snapshot();
        assert_s0_reads_initial(&s0);

        let c1 = vec![
            (b"k1".to_vec(), Some(b"new_value1".to_vec())),
            (b"k2".to_vec(), None),
        ];
        snapshot_index
            .add_generation(&c1.iter().map(|(k, _v)| k.clone()).collect::<Vec<_>>())
            .unwrap();
        db.write_atomically(c1).unwrap();
        assert_eq!(snapshot_index.inner.read().unwrap().current_generation, 2);

        assert_s0_reads_initial(&s0);
        let s1a = snapshot_index.snapshot();
        let s1b = snapshot_index.snapshot();
        assert_s0_reads_initial(&s0);

        fn assert_s1_reads_updates(s1: &Snapshot) {
            assert_eq!(s1.generation, 2);
            assert_eq!(s1.get(b"k1").unwrap(), Some(b"new_value1".to_vec()));
            assert_eq!(s1.get(b"k2").unwrap(), None);
            assert_eq!(s1.find_by_prefix(b"k").unwrap().len(), 2);
        }
        assert_s1_reads_updates(&s1a);
        assert_s1_reads_updates(&s1b);

        snapshot_index.add_generation(&[b"k1".to_vec()]).unwrap();
        assert_eq!(snapshot_index.inner.read().unwrap().current_generation, 3);
        dbg!(&snapshot_index);

        {
            let si_guard = snapshot_index.inner.read().unwrap();
            assert_eq!(si_guard.active_generations[&1].references, 1);
            assert_eq!(si_guard.active_generations[&2].references, 2);
        }
        drop(s0);

        {
            let si_guard = snapshot_index.inner.read().unwrap();
            assert!(si_guard.active_generations.get(&1).is_none());
            assert_eq!(si_guard.active_generations[&2].references, 2);
        }
        drop(s1a);
        {
            let si_guard = snapshot_index.inner.read().unwrap();
            assert_eq!(si_guard.active_generations[&2].references, 1);
        }
        drop(s1b);
        {
            let si_guard = snapshot_index.inner.read().unwrap();
            assert!(si_guard.active_generations.is_empty());
        }
    }
}
