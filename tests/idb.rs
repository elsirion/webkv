use std::sync::Arc;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use webkv::{Database, IdbStorage};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_basic_ops() {
    let db = Database::new(Arc::new(IdbStorage::new("test", "test").await.unwrap()));
    let mut tx = db.transaction().await;

    // Can read empty DB
    let all_k_keys = tx.find_by_prefix(&[]).await.unwrap();
    assert_eq!(all_k_keys.len(), 0);

    tx.set(b"key1".into(), b"value1".into());
    tx.set(b"key2".into(), b"value2".into());
    tx.set(b"xkey".into(), b"valuex".into());
    tx.commit().await.unwrap();

    let mut tx = db.transaction().await;
    assert_eq!(tx.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    assert_eq!(tx.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    assert_eq!(tx.get(b"key3").await.unwrap(), None);

    let all_k_keys = tx.find_by_prefix(b"k").await.unwrap();
    assert_eq!(all_k_keys.len(), 2);
    assert_eq!(all_k_keys[0], (b"key1".to_vec(), b"value1".to_vec()));
    assert_eq!(all_k_keys[1], (b"key2".to_vec(), b"value2".to_vec()));
}
