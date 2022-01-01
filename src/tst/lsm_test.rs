#[cfg(test)]
use crate::storage::lsm::LsmTree;

#[allow(unused_imports)]
use super::tst_util::verify_key_value;

#[test]
pub fn test_lsm_basic() {
    let dbname = "test_lsm_basic";
    let mut lsm = LsmTree::new_delete_existing(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let result = lsm.write(k, v);

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
    let mut lsm = LsmTree::new_delete_existing(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let result= lsm.write(k, v);

    assert!(result, "Failed to write <foo, bar> to lsm");

    if let Some(value) = lsm.get(k) {
        assert!(value == v, "Expected {} for value of {}, actually {}", v, k, value);
    }
    else {
        panic!("Failed to get key value pair for key foo");
    }

    let mut lsm_new_delete_existing = LsmTree::new_delete_existing(dbname);

    if let Some(_) = lsm_new_delete_existing.get("foo") {
        panic!("Failed to delete existing DB, found value for foo on new DB");
    }
}

#[test]
pub fn test_lsm_overwrite_value() {
    let dbname = "test_lsm_overwrite_value";
    let mut lsm = LsmTree::new_delete_existing(dbname);
    let (k, v) = ("foo", "bar");

    // write <foo, bar> to tree
    let result= lsm.write(k, v);
    if result {
        verify_key_value(&mut lsm, k, v);
    }

    // shadow v and replace original k-v pair
    let v = "bar2";

    // write <foo, bar2> to tree, would expect to overwrite existing pair
    let result = lsm.write(k, v);
    if result {
        verify_key_value(&mut lsm, k, v);
    }
}

#[test]
pub fn test_lsm_restore_from_log() {
    let mut lsm = LsmTree::new_delete_existing("test_lsm_restore_from_log");

    for i in 0..lsm.num_entries() {
        let (k, v) = (format!("foo{}", i), format!("bar{}", i));
        lsm.write(&k, &v);
    }

    // shadowing lsm with same DB name is equivalent to re-starting process and
    // spinning up an existing DB, internally, it should result in restoring from
    // the existing lo (test_lsm_restore_from_log.log), rather than creating a fresh LSM
    let mut lsm = LsmTree::new("test_lsm_restore_from_log");

    for i in 0..lsm.num_entries() {
        let (k, v) = (format!("foo{}", i), format!("bar{}", i));
        lsm.write(&k, &v);
        verify_key_value(&mut lsm, &k, &v);
    }
}

#[test]
pub fn test_lsm_get_tuples_from_old_segments() {
    /*
    The goal of this test is to validate the enhancements made to persist full log segments to disk.
    We would expect that any tuples on an old log segment would be found were we to try to get these,
    and also that we preserve the append-only log approach, where log segments are traversed in newest
    to oldest order (with no duplicates in a segment), meaning the latest value for a particular key
    is what is respected, as opposed values for the same key in older segments
    */
    let mut lsm = LsmTree::new_delete_existing("test_lsm_get_tuples_from_old_segments");
    let mut i = 0;

    // Keep appending to the lsm until we persist the current log 
    while lsm.total_segments() == 0 {
        let (k, v) = (format!("foo{}", i), format!("bar{}", i));
        lsm.write(&k, &v);
        verify_key_value(&mut lsm, &k, &v);
        i += 1;
    }

    let ex_segments = 1;
    let ex_tree_size = 1;
    assert!(lsm.total_segments() == ex_segments, "expected {} disk segments, actually {}", ex_segments, lsm.total_segments());
    assert!(lsm.num_entries() == ex_tree_size, "expected {} entries in tree, actually {}", ex_tree_size, lsm.num_entries());

    // Check all of the keys which are now in an old segment
    for j in 0..i {
        let (k, v) = (format!("foo{}", j), format!("bar{}", j));
        verify_key_value(&mut lsm, &k, &v);
    }
}

#[test]
pub fn test_lsm_verify_reclaim_old_segments() {
    /*
    The goal of this test is to validate the enhancements made to persist full log segments to disk.
    We would expect that any tuples that are updated reflect the value in the latest log segment,
    and any prior values for the key are ignored
    */
    let mut lsm = LsmTree::new_delete_existing("test_lsm_verify_reclaim_old_segments");
    let mut i = 0;

    // Keep appending to the lsm until we persist the current log 
    while lsm.total_segments() == 0 {
        let (k, v) = (format!("foo{}", i), format!("bar{}", i));
        lsm.write(&k, &v);
        verify_key_value(&mut lsm, &k, &v);
        i += 1;
    }

    // We flush the in-memory segment lazily, so append one more key to force this
    let (k, v) = (format!("foo{}", i), format!("bar{}", i));
    lsm.write(&k, &v);
    verify_key_value(&mut lsm, &k, &v);

    let mut i = 0;

    // Replace all keys from above with new values
    while lsm.total_segments() == 1 {
        let (k, v) = (format!("foo{}", i), format!("zar{}", i));
        lsm.write(&k, &v);
        verify_key_value(&mut lsm, &k, &v);
        i += 1;
    }

    // We flush the in-memory segment lazily, so append one more key to force this
    let (k, v) = (format!("foo{}", i), format!("zar{}", i));
    lsm.write(&k, &v);
    verify_key_value(&mut lsm, &k, &v);

    let ex_segments = 2;
    let ex_tree_size = 2;
    assert!(lsm.total_segments() == ex_segments, "expected {} disk segments, actually {}", ex_segments, lsm.total_segments());
    assert!(lsm.num_entries() == ex_tree_size, "expected {} entries in tree, actually {}", ex_tree_size, lsm.num_entries());

    // First tree will have values foo0 to foo{tree size - 1}, second tree will
    // have values foo{tree size} to foo{tree size - 2}, 
    // in memory segment will have value foo{tree size - 1} and foo{tree size}
    for j in 0..i {
        let (k, v) = (format!("foo{}", j), format!("zar{}", j));
        verify_key_value(&mut lsm, &k, &v);
    }
}
/* 
#[test]
pub fn test_lsm_tombstone_existing_value() {
    /*
    The goal of this test is to validate the enhancements made to persist full log segments to disk.
    We would expect that any tuples on an old log segment would be found were we to try to get these,
    and also that we preserve the append-only log approach, where log segments are traversed in newest
    to oldest order (with no duplicates in a segment), meaning the latest value for a particular key
    is what is respected, as opposed values for the same key in older segments
    */
    let mut lsm = LsmTree::new_delete_existing("test_lsm_tombstone_existing_value");
    let mut i = 0;

    // Keep appending to the lsm until we persist the current log 
    while lsm.total_segments() == 0 {
        let (k, v) = (format!("foo{}", i), format!("bar{}", i));
        lsm.write(&k, &v);
        verify_key_value(&mut lsm, &k, &v);
        i += 1;
    }

    // We flush the in-memory segment lazily, so append one more key to force this
    let (k, v) = (format!("foo{}", i), format!("bar{}", i));
    lsm.write(&k, &v);
    verify_key_value(&mut lsm, &k, &v);

    let mut i = 0;

    // Replace all keys from above with new values
    while lsm.total_segments() == 1 {
        let (k, v) = (format!("foo{}", i), format!("zar{}", i));
        lsm.write(&k, &v);
        verify_key_value(&mut lsm, &k, &v);
        i += 1;
    }

    // We flush the in-memory segment lazily, so append one more key to force this
    let (k, v) = (format!("foo{}", i), format!("bar{}", i));
    lsm.write(&k, &v);
    verify_key_value(&mut lsm, &k, &v);

    let mut i = 0;

    let ex_segments = 2;
    let ex_tree_size = 1;
    assert!(lsm.total_segments() == ex_segments, "expected {} disk segments, actually {}", ex_segments, lsm.total_segments());
    assert!(lsm.num_entries() == ex_tree_size, "expected {} entries in tree, actually {}", ex_tree_size, lsm.num_entries());

    // Check all of the keys which are now in an old segment. Note we should prioritize
    // the newest log segment when searching for the value (values of zar*)
    while lsm.total_segments() == 0 {
        let (k, v) = (format!("foo{}", i), format!("zar{}", i));
        verify_key_value(&mut lsm, &k, &v);
        i += 1;
    }
}
*/