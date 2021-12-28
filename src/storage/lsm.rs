use std::io::Write;
use std::{path::Path, fs::{OpenOptions, File}};
use crate::storage::tree::*;
use crate::kvpair::*;

pub struct LsmTree<'a> {
    name: &'a str,
    log_file: File,
    tree: LogSegment<&'a str>
}

impl<'a> LsmTree<'a> {
    // new: should take tree (DB) name, and generate log file
    // for the DB based on name, return back structure with new
    // BST and log file path
    pub fn new(name: &str) -> LsmTree {
        // Check for existing log for this DB, then we are not creating new DB and should
        // restore log to memory
        let path_str = format!("{}.log", name);
        let log_file = Path::new(&path_str);
        if Path::exists(log_file) {
            return LsmTree{name, log_file: OpenOptions::new().write(true).truncate(false).open(log_file).unwrap(), tree: LogSegment::new()};
        }
        LsmTree{name, log_file: File::create(log_file).unwrap(), tree: LogSegment::new()}
    }

    fn log<'b>(&mut self, entry: &'b KVPair) -> bool {
        match self.log_file.write(format!("{}\n", entry).as_bytes()) {
            Err(e) => {println!("failed to log {} to db {} with error {}", entry, self.name, e); false}
            Ok(_) => {println!("logged entry for {} for db {}", entry, self.name); true}
        }
    }

    pub fn write<'b>(mut self, key: &'a str, value: &'a str) -> (LsmTree<'a>, bool) {
        let kvp = KVPair::new(key, value);
        match self.log(&kvp) {
            true => {
                self.tree = self.tree.insert((key, value));
                (self, true)
            },
            false => {(self, false)}
        }
    }

    pub fn get(&self, key: &'a str) -> Option<&'a str> {
        return self.tree.get(key);
    }
}