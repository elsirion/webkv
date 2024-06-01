pub mod memory;

use crate::{async_trait_maybe_send, maybe_add_send_sync, Key, KeyRef, Value};
use macro_rules_attribute::apply;
use std::sync::Arc;

pub type AtomicStorage = Arc<maybe_add_send_sync!(dyn IAtomicStorage)>;

/// Database that allows atomic writes
///
/// The interface is designed with IndexedDB in mind, but kept somewhat generic to potentially support other databases in the future.
#[apply(async_trait_maybe_send)]
pub trait IAtomicStorage {
    async fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>>;

    async fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>>;

    async fn write_atomically(&self, changes: Vec<(Key, Option<Value>)>) -> anyhow::Result<()>;
}
