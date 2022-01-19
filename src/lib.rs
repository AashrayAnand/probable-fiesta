pub mod storage {
    pub mod tree;
    pub mod lsm;
    pub mod diskseg;
    pub mod files;
}

pub mod replication {
    pub mod network;
}

pub mod tst {
    // declaring module inline and placing all tests there as per this convention
    // https://stackoverflow.com/questions/58935890/how-to-import-from-a-file-in-a-subfolder-of-src
    pub mod bst_test;
    pub mod lsm_test;
    pub mod tst_util;
}

pub const LOGGING: bool = false;
pub const LOCALHOST: &str = "127.0.0.1";
pub const SRV_PORT: u16 = 3620;
pub const CLI_PORT: u16 = 8942;

pub fn log(msg: &str) {
    if LOGGING {
        println!("{}", msg);
    }
}