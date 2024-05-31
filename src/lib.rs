#![allow(clippy::arc_with_non_send_sync)]

pub(crate) mod snapshot;
pub(crate) mod storage;
mod transaction;
pub(crate) mod util;

#[cfg(target_family = "wasm")]
pub use storage::idb::IdbStorage;
pub use storage::{AtomicStorage, IAtomicStorage};
pub use transaction::{Database, Transaction};

pub type Key = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];

pub type Value = Vec<u8>;
pub type ValueRef<'a> = &'a [u8];
