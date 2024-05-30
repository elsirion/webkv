pub mod memory;

use crate::{maybe_add_send_sync, Key, KeyRef, Value};
use std::sync::Arc;

pub type AtomicStorage = Arc<maybe_add_send_sync!(dyn IAtomicStorage)>;

/// Database that allows atomic writes
///
/// The interface is designed with IndexedDB in mind, but kept somewhat generic to potentially support other databases in the future.
pub trait IAtomicStorage {
    fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>>;

    fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>>;

    fn write_atomically(&self, changes: Vec<(Key, Option<Value>)>) -> anyhow::Result<()>;
}
