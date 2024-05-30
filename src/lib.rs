pub(crate) mod snapshot;
pub(crate) mod storage;
mod transaction;

pub type Key = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];

pub type Value = Vec<u8>;
pub type ValueRef<'a> = &'a [u8];
