use storage::{lsm::LsmTree, tree::LogSegment};

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
    let k = "foo";
    let mut l = LsmTree::new("x");

    {
        let g = "asd";
        l = LsmTree::new(g);
    }

    let (l, result) = l.write(k, "bar");
    if result {
        if let Some(v) = l.get(k) {println!("Value for {} is {}", k, v)}
    }
}
