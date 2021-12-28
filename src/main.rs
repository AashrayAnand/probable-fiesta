use storage::lsm::LsmTree;

pub mod kvpair;
pub mod operators;
pub mod config;

pub mod storage {
    pub mod tree;
    pub mod lsm;
}

pub mod tst {
    // declaring module inline and placing all tests there as per this convention
    // https://stackoverflow.com/questions/58935890/how-to-import-from-a-file-in-a-subfolder-of-src
    pub mod bst_test;
    pub mod lsm_test;
    pub mod operators_test;
}

fn main() {
    let l = LsmTree::new("foo");

    let (l, result) = l.write("foo", "bar");
    if result {
        let _ = l.get("foo");
    }
}
