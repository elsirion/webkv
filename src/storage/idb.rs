use crate::{async_trait_maybe_send, IAtomicStorage, Key, KeyRef, Value};
use macro_rules_attribute::apply;
use idb::{TransactionMode, Query, CursorDirection, KeyRange, ObjectStoreParams, DatabaseEvent};
use wasm_bindgen::JsValue;
use anyhow::Context;

pub struct IdbStorage {
    db: idb::Database,
    store_name: String,
}

impl IdbStorage {
    pub async fn new(db_name: &str, store_name: &str) -> anyhow::Result<Self> {
        // TODO: prevent opening the same store multiple times
        let idb_factory = idb::Factory::new().map_err(idb_error_to_anyhow)?;

        let mut db_req = idb_factory
            .open(db_name, Some(1))
            .map_err(idb_error_to_anyhow)?;

        let store_name_inner = store_name.to_owned();
        db_req.on_upgrade_needed(move |vc_event| {
            let db = vc_event.database().expect("DB not found in upgrade event");
            let mut object_store_params = ObjectStoreParams::new();
            object_store_params.auto_increment(false);
            db.create_object_store(&store_name_inner, object_store_params).expect("Failed to create object store");
        });

        let db = db_req.await
            .map_err(idb_error_to_anyhow)?;

        Ok(Self {
            db,
            store_name: store_name.to_owned(),
        })
    }
}

#[apply(async_trait_maybe_send)]
impl IAtomicStorage for IdbStorage {
    async fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
        let hex_key = hex::encode(key);

        let dbtx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadOnly)
            .map_err(idb_error_to_anyhow)?;
        let result = dbtx
            .object_store(&self.store_name)
            .map_err(idb_error_to_anyhow)?
            .get(Query::Key(JsValue::from(&hex_key)))
            .map_err(idb_error_to_anyhow)?
            .await
            .map_err(idb_error_to_anyhow)?;

        match result {
            None => return Ok(None),
            Some(raw_value) => {
                let hex_value = raw_value
                    .as_string()
                    .context("IndexedDB returned non-string value, storage corrupted")?;
                Ok(Some(hex::decode(hex_value)?))
            }
        }
    }

    async fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        let hex_prefix = hex::encode(prefix);
        let dbtx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadOnly)
            .map_err(idb_error_to_anyhow)?;
        let cursor = dbtx
            .object_store(&self.store_name)
            .map_err(idb_error_to_anyhow)?
            .open_cursor(
                Some(Query::KeyRange(
                    KeyRange::lower_bound(&JsValue::from(&hex_prefix), Some(false))
                        .map_err(idb_error_to_anyhow)?,
                )),
                Some(CursorDirection::Next),
            )
            .map_err(idb_error_to_anyhow)?
            .await
            .map_err(idb_error_to_anyhow)?
            .context("IndexedDB didn't return cursor")?;

        // TODO: maybe make iterator?
        let mut result = Vec::new();
        loop {
            let raw_key = cursor.key().map_err(idb_error_to_anyhow)?;
            let raw_value = cursor.value().map_err(idb_error_to_anyhow)?;

            if raw_key.is_undefined() {
                break;
            }

            let key = hex::decode(
                raw_key
                    .as_string()
                    .context("IndexedDB returned non-string key, storage corrupted")?,
            )?;
            let value = hex::decode(
                raw_value
                    .as_string()
                    .context("IndexedDB returned non-string value, storage corrupted")?,
            )?;

            if !key.starts_with(prefix) {
                break;
            }

            result.push((key, value));
            cursor.next(None).map_err(idb_error_to_anyhow)?.await.map_err(idb_error_to_anyhow)?;
        }

        Ok(result)
    }

    async fn write_atomically(&self, changes: Vec<(Key, Option<Value>)>) -> anyhow::Result<()> {
        let dbtx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadWrite)
            .map_err(idb_error_to_anyhow)?;

        for (key, value) in changes {
            match value {
                Some(value) => {
                    let hex_key = hex::encode(&key);
                    let hex_value = hex::encode(&value);
                    dbtx.object_store(&self.store_name)
                        .map_err(idb_error_to_anyhow)?
                        .put(&JsValue::from(&hex_value), Some(&JsValue::from(&hex_key)))
                        .map_err(idb_error_to_anyhow)?
                        .await
                        .map_err(idb_error_to_anyhow)?;
                }
                None => {
                    let hex_key = hex::encode(&key);
                    dbtx.object_store(&self.store_name)
                        .map_err(idb_error_to_anyhow)?
                        .delete(JsValue::from(&hex_key))
                        .map_err(idb_error_to_anyhow)?
                        .await
                        .map_err(idb_error_to_anyhow)?;
                }
            }
        }

        dbtx.commit().map_err(idb_error_to_anyhow)?;

        Ok(())
    }
}

fn idb_error_to_anyhow(err: idb::Error) -> anyhow::Error {
    anyhow::anyhow!("IndexedDB error: {:?}", err)
}
