use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use embed_doc_image::embed_doc_image;
use futures_locks::RwLock;
use itertools::{merge_join_by, EitherOrBoth};
use tracing::debug;

use crate::storage::AtomicStorage;
use crate::util::spawn;
use crate::{Key, KeyRef, Value};

pub type Generation = u64;

/// An index of all the overwritten keys and their previous values that are
/// still being referenced by active [`Snapshot`]s. We use the concept of
/// [`Generation`]s to track atomic changes to the database. Think of them as
/// the result of a commit.
///
/// # Implementation
/// The index is implemented as a [`BTreeMap`] of [`Key`]s to a [`BTreeMap`] of
/// [`Generation`]s to **their previous** [`Value`]s stored as `Option<Value>`
/// since the previous value might have been undefined/absent. These two layers
/// allow:
///   * Efficient prefix-search of keys
///   * Efficient lookup of the next-larger generation of a key than a given
///     snapshot generation
///
/// The emphasis on "*their previous*" is important here, because the value of a
/// key in the snapshot index is the value that was present in the database
/// before the generation was committed. So when looking for the values as it
/// would have been returned by the database at generation `x` we look at all
/// generations in `(x+1)..`.
///
/// ## Adding Generations
/// When a new generations is created using
/// [`LazySnapshotIndex::add_generation`] the snapshot index will increment the
/// `current_generation`, fetch the values of all keys that were mutated in that
/// generation and store them in the index under the new `current_generation`.
///
/// After a new generation is created the caller is expected to atomically write
/// the new values to the database, while this isn't the job of the snapshot
/// index, it is important to know for the following explanations.
///
/// ![Adding a generation to the index][new_gen]
///
/// ## Adding snapshots
/// Adding a snapshot is done by calling [`LazySnapshotIndex::snapshot`]. This
/// creates or increments an entry in the [`LazySnapshotIndexInner::snapshots`]
/// map. The snapshot generation is the `current_generation` at the time the
/// snapshot was created. The entry in `snapshots` prevents generations
/// necessary to look up values from the snapshot from being removed from the
/// index.
///
/// ## Looking up values from a snapshot
/// When looking up a value from a snapshot we first check if the key exists in
/// [`LazySnapshotIndexInner::generation_prev_values`]. If so we look for the
/// first generation that is strictly higher than the snapshot generation and
/// return the value from that generation. If the key isn't found in the index
/// we look up the value from the database.
///
/// Note that the lookup in `generation_prev_values` may return `Some(None)`,
/// which means there is an entry with value none, indicating that at time of
/// the snapshot the key didn't exist in the database, but was later written to.
/// In that case `None` is returned.
///
/// Some examples for snapshot key value lookups:
///
/// ![Snapshot lookup examples][snapshot_lookup]
///
/// ## Dropping snapshots
/// When a snapshot is dropped the generation is sent to a background worker
/// that will decrement the respective `sessions` entry and remove it if zero.
/// It then looks up the oldest generation `g` that still has an active snapshot
/// and removes all generations that are lower (=older) than `g+1` from the
/// index. The `+1` stems from the backed up values overwritten in `g` not being
/// needed for lookups of keys in session `g`.
///
/// When removing a generation the [`LazySnapshotIndexInner::generation_keys`]
/// index is used to determine which keys need to be visited to remove
/// generation value backups from. Once all keys have been visited the
/// generation is removed from the `generation_keys` index.
///
/// ![Snapshots becoming unreferenced and generations being
/// removed][snapshot_removal]
#[embed_doc_image("new_gen", "images/new_gen.png")]
#[embed_doc_image("snapshot_lookup", "images/snapshot_lookup.png")]
#[embed_doc_image("snapshot_removal", "images/snapshot_removal.png")]
pub struct LazySnapshotIndex {
    inner: Arc<RwLock<LazySnapshotIndexInner>>,
    /// We cannot call async code in `Drop::drop`, so we send cleanup jobs to a
    /// background worker task
    drop_snapshot_sender: tokio::sync::mpsc::UnboundedSender<Generation>,
}

struct LazySnapshotIndexInner {
    /// Database reference to read values from if key isn't found in the
    /// snapshot and when creating value backups.
    db: AtomicStorage,
    /// Each commit to the DB creates a new generation. This counter references
    /// the current one.
    current_generation: Generation,
    /// Generations for which there are active snapshots. The value is the
    /// number of active snapshots. Note that the snapshot generation is the
    /// `current_generation` when the snapshot was created, this means we only
    /// look for values from generations strictly higher than that when looking
    /// up keys.
    snapshots: BTreeMap<Generation, usize>,
    /// For each generation the keys that were mutated in that generation. This
    /// is a performance optimization to avoid having to scan all keys in the DB
    /// when deleting a generation.
    generation_keys: BTreeMap<Generation, HashSet<Key>>,
    /// For each key the generation maps to the previous value that was
    /// overwritten by that generation. When looking up a key, the first value
    /// with a strictly higher generation than the snapshot generation is
    /// returned.
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

    /// Has to be called before writing changes to the database to generate a
    /// sparse snapshot of the current DB state.
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

        // Now that we have removed a reference to the generation we can check if it can
        // be removed. We only remove from oldest to newest since removing generations
        // in the middle would require complicated checks if some of the cached keys
        // held by it might still be needed.

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

        // Collect all keys that we need to visit since they were mutated in the
        // removable generations and thus hold value backups for older snapshots to read
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

    /// Check if any newer generations than the one the snapshot was created in
    /// have mutated the `keys` supplied.
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
    use std::future::Future;
    use std::sync::Arc;

    use crate::snapshot::{LazySnapshotIndex, Snapshot};
    use crate::storage::memory::MemStorage;
    use crate::storage::IAtomicStorage;

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
        F: Future<Output = bool>,
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
