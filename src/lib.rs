pub(crate) mod snapshot;
pub(crate) mod storage;
mod transaction;
pub(crate) mod util;

pub use transaction::{Database, Transaction};
pub use storage::memory::MemStorage;
pub use storage::{AtomicStorage, IAtomicStorage};

pub type Key = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];

pub type Value = Vec<u8>;
pub type ValueRef<'a> = &'a [u8];
