use std::collections::BTreeMap;
use std::sync::Mutex;

use macro_rules_attribute::apply;

use crate::storage::IAtomicStorage;
use crate::{async_trait_maybe_send, Key, KeyRef, Value};

#[derive(Default, Debug)]
pub struct MemStorage {
    data: Mutex<BTreeMap<Key, Value>>,
}

#[apply(async_trait_maybe_send)]
impl IAtomicStorage for MemStorage {
    async fn get(&self, key: KeyRef<'_>) -> anyhow::Result<Option<Value>> {
        delay_in_test().await;

        Ok(self.data.lock().expect("poisoned").get(key).cloned())
    }

    async fn find_by_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Key, Value)>> {
        delay_in_test().await;

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

    async fn write_atomically(&self, changes: Vec<(Key, Option<Value>)>) -> anyhow::Result<()> {
        delay_in_test().await;

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

/// Simulate yielding execution to the runtime and awaiting some result from an
/// async operation like an IndexedDB query.
#[cfg(test)]
async fn delay_in_test() {
    use rand::{thread_rng, Rng};
    let random_delay_us = thread_rng().gen_range(500..1500);
    tokio::time::sleep(std::time::Duration::from_micros(random_delay_us)).await;
}

#[cfg(not(test))]
async fn delay_in_test() {}
