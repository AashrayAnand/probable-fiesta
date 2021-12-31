use crate::storage::lsm::LsmTree;
pub fn verify_key_value(tree: &mut LsmTree, k: &str, v: &str) {
    if let Some(value) = tree.get(k) {
        println!("got value {} for key {}", value, k);
        assert!(v == value, "invalid key, expected {}, actually {}", v, value);
    }
    else {
        panic!("No value found for key {}", k);
    }
}