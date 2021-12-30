use crate::storage::lsm::LsmTree;
#[cfg(test)]

pub fn verify_key_value(tree: &LsmTree, k: &'static str, v: &'static str) {
    if let Some(value) = tree.get(k) {
        println!("got value {} for key {}", value, k);
        assert!(v == value, "invalid key, expected {}, actually {}", v, value);
    }
    else {
        panic!("No value found for key {}", k);
    }
}

#[test]
pub fn test_lsm_basic() {
    let dbname = "test_lsm_basic";
    let lsm = LsmTree::new_delete_existing(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let (lsm, result) = lsm.write(k, v);

    assert!(result, "Failed to write <foo, bar> to lsm");

    if let Some(value) = lsm.get(k) {
        assert!(value == v, "Expected {} for value of {}, actually {}", v, k, value);
    }
    else {
        panic!("Failed to get key value pair for key foo");
    }
}

#[test]
pub fn test_lsm_create_delete_existing() {
    let dbname = "test_lsm_create_and_delete";
    let lsm = LsmTree::new_delete_existing(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let (lsm, result) = lsm.write(k, v);

    assert!(result, "Failed to write <foo, bar> to lsm");

    if let Some(value) = lsm.get(k) {
        assert!(value == v, "Expected {} for value of {}, actually {}", v, k, value);
    }
    else {
        panic!("Failed to get key value pair for key foo");
    }

    let lsm_new_delete_existing = LsmTree::new_delete_existing(dbname);

    if let Some(_) = lsm_new_delete_existing.get("foo") {
        panic!("Failed to delete existing DB, found value for foo on new DB");
    }
}

#[test]
pub fn test_lsm_overwrite_value() {
    let dbname = "test_lsm_overwrite_value";
    let lsm = LsmTree::new_delete_existing(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let (lsm, result) = lsm.write(k, v);
    if result {
        verify_key_value(&lsm, k, v);
    }

    // shadow v and replace original k-v pair
    let v = "bar2";

    // write <foo, bar2> to tree, would expect to overwrite existing pair
    let (lsm, result) = lsm.write(k, v);
    if result {
        verify_key_value(&lsm, k, v);
    }
}

#[test]
pub fn test_lsm_restore_from_log() {
    let lsm = LsmTree::new_delete_existing("test_lsm_restore_from_log");

    let (k, v) = ("foo", "bar");
    let (lsm, _) = lsm.write(k, v);

    let (k, v) = ("foo1", "bar1");
    let (lsm, _) = lsm.write(k, v);
    
    let (k, v) = ("foo2", "bar2");
    let (lsm, _) = lsm.write(k, v);

    let (k, v) = ("foo3", "bar3");
    let (lsm, _) = lsm.write(k, v);
    
    let (k, v) = ("foo4", "bar4");
    let (lsm, _) = lsm.write(k, v);

    let (k, v) = ("foo5", "bar5");
    let (_, _) = lsm.write(k, v);

    // shadowing lsm with same DB name is equivalent to re-starting process and
    // spinning up an existing DB, internally, it should result in restoring from
    // the existing lo (test_lsm_restore_from_log.log), rather than creating a fresh LSM
    let lsm = LsmTree::new("test_lsm_restore_from_log");

    verify_key_value(&lsm, "foo", "bar");
    verify_key_value(&lsm, "foo1", "bar1");
    verify_key_value(&lsm, "foo2", "bar2");
    verify_key_value(&lsm, "foo3", "bar3");
    verify_key_value(&lsm, "foo4", "bar4");
    verify_key_value(&lsm, "foo5", "bar5");
}
