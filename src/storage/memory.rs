use crate::storage::IAtomicStorage;
use crate::{Key, KeyRef, Value};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default, Debug)]
pub struct MemStorage {
    data: Mutex<BTreeMap<Key, Value>>,
}

impl IAtomicStorage for MemStorage {
    fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
        Ok(self.data.lock().expect("poisoned").get(key).cloned())
    }

    fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        let prefix = prefix.to_vec();
        let result = self
            .data
            .lock()
            .expect("poisoned")
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(result)
    }

    fn write_atomically(&self, changes: Vec<(Key, Option<Value>)>) -> anyhow::Result<()> {
        let mut db = self.data.lock().expect("poisoned");
        for (key, maybe_value) in changes {
            match maybe_value {
                Some(value) => {
                    db.insert(key, value);
                }
                None => {
                    db.remove(&key);
                }
            }
        }
        Ok(())
    }
}
