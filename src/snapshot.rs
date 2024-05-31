use crate::storage::AtomicStorage;
use crate::util::spawn;
use crate::{Key, KeyRef, Value};
use futures_locks::RwLock;
use itertools::{merge_join_by, EitherOrBoth};
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tracing::debug;

pub type Generation = u64;

pub struct LazySnapshotIndex {
    inner: Arc<RwLock<LazySnapshotIndexInner>>,
    /// We cannot call async code in `Drop::drop`, so we send cleanup jobs to a background worker task
    drop_snapshot_sender: tokio::sync::mpsc::UnboundedSender<Generation>,
}

struct LazySnapshotIndexInner {
    db: AtomicStorage,
    current_generation: Generation,
    snapshots: BTreeMap<Generation, usize>,
    generation_keys: BTreeMap<Generation, HashSet<Key>>,
    /// For each key the generation maps to the previous value that was overwritten by that generation.
    generation_prev_values: BTreeMap<Key, BTreeMap<Generation, Option<Value>>>,
}

/// Explicitly not cloneable
pub struct Snapshot {
    generation: Generation,
    inner: Arc<RwLock<LazySnapshotIndexInner>>,
    drop_snapshot_sender: tokio::sync::mpsc::UnboundedSender<Generation>,
}

impl LazySnapshotIndex {
    pub fn new(db: AtomicStorage) -> Self {
        let (drop_snapshot_sender, mut drop_snapshot_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(RwLock::new(LazySnapshotIndexInner {
            db,
            current_generation: 1,
            snapshots: Default::default(),
            generation_keys: Default::default(),
            generation_prev_values: Default::default(),
        }));

        let inner_clone = inner.clone();
        spawn("snapshot_cleanup", async move {
            while let Some(generation) = drop_snapshot_receiver.recv().await {
                debug!("Dropping snapshot generation {}", generation);
                inner_clone.write().await.remove_snapshot(generation);
            }
        });

        Self {
            inner,
            drop_snapshot_sender,
        }
    }

    pub async fn snapshot(&self) -> Snapshot {
        let mut inner = self.inner.write().await;
        let generation = inner.current_generation;
        let snapshot_entry = inner.snapshots.entry(generation).or_insert(0);
        *snapshot_entry += 1;

        Snapshot {
            generation,
            inner: self.inner.clone(),
            drop_snapshot_sender: self.drop_snapshot_sender.clone(),
        }
    }

    /// Has to be called before writing changes to the database to generate a sparse snapshot of the current DB state.
    pub async fn add_generation(&self, changes: &[Key]) -> anyhow::Result<()> {
        // FIXME: don't write if no transactions are active
        // TODO: maybe even filter out keys that are shadowed anyway

        let mut inner = self.inner.write().await;
        inner.current_generation += 1;
        let current_generation = inner.current_generation;

        inner
            .generation_keys
            .insert(current_generation, changes.iter().cloned().collect());

        for key in changes {
            let maybe_value = inner.db.get(key).await?;
            inner
                .generation_prev_values
                .entry(key.clone())
                .or_default()
                .insert(current_generation, maybe_value);
        }

        Ok(())
    }
}

impl LazySnapshotIndexInner {
    fn remove_snapshot(&mut self, generation: Generation) {
        {
            let snapshot_references = self
                .snapshots
                .get_mut(&generation)
                .expect("Generation not found");
            assert_ne!(*snapshot_references, 0);
            *snapshot_references -= 1;
        }

        // Now that we have removed a reference to the generation we can check if it can be removed. We only remove from oldest to newest since removing generations in the middle would require complicated checks if some of the cached keys held by it might still be needed.

        let removable_snapshot_generations = self
            .snapshots
            .iter()
            .take_while(|(_generation, references)| **references == 0)
            .map(|(generation, _)| *generation)
            .collect::<Vec<_>>();

        for snapshot_generation in removable_snapshot_generations {
            self.snapshots.remove(&snapshot_generation);
        }

        let max_removable_key_generation = self
            .snapshots
            .keys()
            .next()
            .copied()
            .unwrap_or(Generation::MAX - 1);
        let removable_key_generations = self
            .generation_keys
            .keys()
            .take_while(|generation| **generation <= max_removable_key_generation)
            .cloned()
            .collect::<Vec<_>>();

        // Collect all keys that we need to visit since they were mutated in the removable generations and thus hold value backups for older snapshots to read
        let keys_to_visit = removable_key_generations
            .iter()
            .flat_map(|generation| self.generation_keys.remove(generation))
            .flatten()
            .collect::<HashSet<_>>();

        for key in keys_to_visit.clone() {
            let key_entry = self
                .generation_prev_values
                .get_mut(&key)
                .expect("Key not found");

            // Remove all value generations that are inside the removable range
            *key_entry = key_entry.split_off(&(max_removable_key_generation + 1));
            if key_entry.is_empty() {
                self.generation_prev_values.remove(&key);
            }
        }
    }
}

impl Debug for LazySnapshotIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Ok(inner) = self.inner.try_read() else {
            return write!(f, "LazySnapshotIndex(locked)");
        };
        f.debug_struct("LazySnapshotIndex")
            .field("current_generation", &inner.current_generation)
            .field("snapshots", &inner.snapshots)
            .field("generation_keys", &inner.generation_keys)
            .field("snapshots", &inner.generation_prev_values)
            .finish()
    }
}

impl Snapshot {
    pub async fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
        let inner = self.inner.read().await;
        let snapshot_result = inner.generation_prev_values.get(key).and_then(|snapshot| {
            snapshot
                .range((self.generation + 1)..)
                .next()
                .map(|(_generation, value)| value.clone())
        });

        match snapshot_result {
            Some(value) => Ok(value),
            None => inner.db.get(key).await,
        }
    }

    pub async fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        let inner = self.inner.read().await;
        let snapshot_prefix_result = inner
            .generation_prev_values
            .range(prefix.to_vec()..)
            .flat_map(|(key, generations)| {
                generations
                    .range((self.generation + 1)..)
                    .next()
                    .map(|(_generation, value)| (key.clone(), value.clone()))
            })
            .collect::<Vec<_>>();

        let db_prefix_result = inner.db.find_by_prefix(prefix).await?;

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
    pub async fn check_conflicts(&self, keys: &HashSet<Key>) -> bool {
        let inner = self.inner.read().await;
        inner.generation_keys.range((self.generation + 1)..).any(
            |(generation_idx, generation_keys)| {
                let conflicts = generation_keys.intersection(keys).collect::<Vec<_>>();

                if !conflicts.is_empty() {
                    debug!(
                        "Conflict on keys: {:?} in generation {}",
                        conflicts, generation_idx
                    );
                }

                !conflicts.is_empty()
            },
        )
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = self.drop_snapshot_sender.send(self.generation);
    }
}

#[cfg(test)]
mod tests {
    use crate::snapshot::{LazySnapshotIndex, Snapshot};
    use crate::storage::memory::MemStorage;
    use crate::storage::IAtomicStorage;
    use std::future::Future;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_snapshot() {
        let db = Arc::new(MemStorage::default());
        let initial_db = vec![
            (b"k1".to_vec(), Some(b"value1".to_vec())),
            (b"k2".to_vec(), Some(b"value2".to_vec())),
            (b"k3".to_vec(), Some(b"value3".to_vec())),
            (b"a1".to_vec(), Some(b"other_value".to_vec())),
        ];
        db.write_atomically(initial_db).await.unwrap();

        let snapshot_index = LazySnapshotIndex::new(db.clone());
        assert_eq!(snapshot_index.inner.read().await.current_generation, 1);

        async fn assert_s0_reads_initial(s0: &Snapshot) {
            assert_eq!(s0.generation, 1);
            assert_eq!(s0.get(b"k1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(s0.find_by_prefix(b"k").await.unwrap().len(), 3);
        }
        let s0 = snapshot_index.snapshot().await;
        assert_s0_reads_initial(&s0).await;

        let c1 = vec![
            (b"k1".to_vec(), Some(b"new_value1".to_vec())),
            (b"k2".to_vec(), None),
        ];
        snapshot_index
            .add_generation(&c1.iter().map(|(k, _v)| k.clone()).collect::<Vec<_>>())
            .await
            .unwrap();
        db.write_atomically(c1).await.unwrap();
        assert_eq!(snapshot_index.inner.read().await.current_generation, 2);

        assert_s0_reads_initial(&s0).await;
        let s1a = snapshot_index.snapshot().await;
        let s1b = snapshot_index.snapshot().await;
        assert_s0_reads_initial(&s0).await;

        async fn assert_s1_reads_updates(s1: &Snapshot) {
            assert_eq!(s1.generation, 2);
            assert_eq!(s1.get(b"k1").await.unwrap(), Some(b"new_value1".to_vec()));
            assert_eq!(s1.get(b"k2").await.unwrap(), None);
            assert_eq!(s1.find_by_prefix(b"k").await.unwrap().len(), 2);
        }
        assert_s1_reads_updates(&s1a).await;
        assert_s1_reads_updates(&s1b).await;

        snapshot_index
            .add_generation(&[b"k1".to_vec()])
            .await
            .unwrap();
        assert_eq!(snapshot_index.inner.read().await.current_generation, 3);

        assert!(
            s0.check_conflicts(&[b"k1".to_vec()].iter().cloned().collect())
                .await
        );
        assert!(
            s0.check_conflicts(&[b"k2".to_vec()].iter().cloned().collect())
                .await
        );
        assert!(
            !s0.check_conflicts(&[b"k3".to_vec()].iter().cloned().collect())
                .await
        );
        assert!(
            !s0.check_conflicts(&[b"kx".to_vec()].iter().cloned().collect())
                .await
        );

        assert!(
            s1a.check_conflicts(&[b"k1".to_vec()].iter().cloned().collect())
                .await
        );
        assert!(
            !s1a.check_conflicts(&[b"k2".to_vec()].iter().cloned().collect())
                .await
        );

        {
            let si_guard = snapshot_index.inner.read().await;
            assert_eq!(si_guard.snapshots[&1], 1);
            assert_eq!(si_guard.snapshots[&2], 2);
            assert!(si_guard.generation_keys.get(&1).is_none());
            assert!(si_guard.generation_keys.get(&2).is_some());
            assert!(si_guard.generation_keys.get(&3).is_some());
        }
        drop(s0);

        assert_s1_reads_updates(&s1a).await;
        assert_s1_reads_updates(&s1b).await;

        wait_for(|| async {
            let si_guard = snapshot_index.inner.read().await;
            si_guard.snapshots.get(&1).is_none()
        })
            .await;
        {
            let si_guard = snapshot_index.inner.read().await;
            assert_eq!(si_guard.snapshots[&2], 2);
            assert!(si_guard.generation_keys.get(&1).is_none());
            assert!(si_guard.generation_keys.get(&2).is_none());
            assert!(si_guard.generation_keys.get(&3).is_some());
        }
        drop(s1a);

        assert_s1_reads_updates(&s1b).await;

        wait_for(|| async {
            let si_guard = snapshot_index.inner.read().await;
            si_guard.snapshots[&2] == 1
        })
            .await;
        {
            let si_guard = snapshot_index.inner.read().await;
            assert!(si_guard.generation_keys.get(&1).is_none());
            assert!(si_guard.generation_keys.get(&2).is_none());
            assert!(si_guard.generation_keys.get(&3).is_some());
        }

        drop(s1b);

        wait_for(|| async {
            let si_guard = snapshot_index.inner.read().await;
            si_guard.snapshots.is_empty()
        })
            .await;
        {
            let si_guard = snapshot_index.inner.read().await;
            assert!(si_guard.generation_prev_values.is_empty());
        }
    }

    async fn wait_for<P, F>(predicate: P)
        where
            P: Fn() -> F,
            F: Future<Output=bool>,
    {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !predicate().await {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
            .await
            .unwrap();
    }
}
