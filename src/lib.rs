#![allow(clippy::arc_with_non_send_sync)]

//! # WebKV - A transactional IndexedDB overlay
//!
//! While IndexedDB supports atomic reads/writes its transaction system is far
//! from sophisticated and e.g. auto-closes transactions when *something else
//! (e.g. a network request) is done in-between two transaction operations. For
//! applications that make use of long-lived transactions this is a
//! deal-breaker.
//!
//! WebKV is a simple overlay that provides **long-lived, optimistic and
//! snapshot-isolated transactions** on top of IndexedDB.
//!
//! ## Usage
//!
//! A [`Database`] can be created from any object implementing
//! [`IAtomicStorage`]. The two default implementations provided in this crate
//! are [`MemStorage`] and [`IdbStorage`]. The former is an in-memory storage
//! that is useful for testing (but potentially also other in-memory KV needs)
//! and the latter is a wrapper around IndexedDB.
//!
//! To interact with the data in the DB you need to create a [`Transaction`].
//! The transaction is a mutable object that can be used to read and write data.
//! The transaction is committed by calling `commit` and rolled back by dropping
//! it without calling `commit`.
//!
//! ```rust
//! use std::sync::Arc;
//! # //#[cfg(target_family = "wasm")]
//! use webkv::{Database, IdbStorage};
//!
//! # //#[cfg(target_family = "wasm")]
//! # tokio_test::block_on(async {
//! let storage_backend = Arc::new(IdbStorage::new("db_name").await.expect("Failed to open IndexedDB"));
//! let db = Database::new(storage_backend);
//!
//! // Check that DB is indeed empty
//! let mut tx = db.transaction().await;
//! let all_k_keys = tx.find_by_prefix(&[]).await.unwrap();
//! assert_eq!(all_k_keys.len(), 0);
//!
//! // Write some values
//! tx.set(b"key1".into(), b"value1".into());
//! tx.set(b"key2".into(), b"value2".into());
//! tx.commit().await.expect("Failed to commit transaction");
//!
//! let mut tx = db.transaction().await;
//! assert_eq!(tx.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
//! assert_eq!(tx.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));//!
//! assert_eq!(tx.get(b"key3").await.unwrap(), None);
//!
//! // Just dropping the transaction without commit will roll back all changes
//! # });
//! ```

pub(crate) mod snapshot;
pub(crate) mod storage;
mod transaction;
pub(crate) mod util;

#[cfg(target_family = "wasm")]
pub use storage::idb::IdbStorage;
pub use storage::memory::MemStorage;
pub use storage::{AtomicStorage, IAtomicStorage};
pub use transaction::{Database, Transaction};

pub type Key = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];

pub type Value = Vec<u8>;
pub type ValueRef<'a> = &'a [u8];
