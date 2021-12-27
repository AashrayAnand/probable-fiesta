#[cfg(test)]
use crate::LsmTree;

#[test]
pub fn test_lsm_basic() {
    let dbname = "test_lsm_basic";
    let lsm = LsmTree::new(dbname);

    // write <foo, bar> to tree
    let (lsm, result) = lsm.write("foo", "bar");

    assert!(result, "Failed to write <foo, bar> to lsm");

    if let Some(value) = lsm.tree.get("foo") {
        assert!(value == "bar", "Expected bar for value of foo, actually {}", value);
    }
    else {
        panic!("Failed to get key value pair for key foo");
    }
}

#[test]
pub fn test_lsm_overwrite_value() {
    let dbname = "test_lsm_overwrite_value";
    let lsm = LsmTree::new(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let (lsm, result) = lsm.write(k, v);
    if result {
        if let Some(value) = lsm.get("foo") {
            println!("got value {} for key {}", value, k);
            assert!(v == value, "invalid key, expected {}, actually {}", v, value);
        }
    }

    // shadow v and replace original k-v pair
    let v = "bar2";

    // write <foo, bar2> to tree, would expect to overwrite existing pair
    let (lsm, result) = lsm.write(k, v);
    if result {
        if let Some(value) = lsm.get("foo") {
            println!("got value {} for key {}", value, k);
            assert!(v == value, "invalid key, expected {}, actually {}", v, value);
        }
    }
}

#[test]
pub fn test_lsm_from_log() {
    let lsm = LsmTree::new("test_lsm_from_log");
    let (k, v) = ("foo", "bar");

    let (lsm, result) = lsm.write(k, v);
    if result {
        if let Some(value) = lsm.get("foo") {
            println!("got value {} for key {}", value, k);
            assert!(v == value, "invalid key, expected {}, actually {}", v, value);
        }
    }

    // shadow v and replace original k-v pair
    let v = "bar2";
    let (lsm, result) = lsm.write(k, v);
    if result {
        if let Some(value) = lsm.get("foo") {
            println!("got value {} for key {}", value, k);
            assert!(v == value, "invalid key, expected {}, actually {}", v, value);
        }
    }
}