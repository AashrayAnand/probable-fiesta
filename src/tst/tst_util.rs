use crate::{storage::lsm::LsmTree, log};
pub fn verify_key_value(tree: &mut LsmTree, k: &str, v: &str) {
    if let Some(value) = tree.get(k) {
        assert!(v == value, "invalid key, expected {}, actually {}", v, value);
        log(&format!("verified {} {}", k, v));
    }
    else {
        panic!("No value found for key {}", k);
    }
}

pub fn verify_deleted(tree: &mut LsmTree, k: &str) {
    if let Some(value) = tree.get(k) {
        panic!("expected deleted key {}, actually {}", k, value);
    }
}