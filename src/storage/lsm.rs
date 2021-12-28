use std::io::{BufReader, Write, BufRead};
use std::{path::Path, fs::{OpenOptions, File}};
use crate::storage::tree::*;
use crate::kvpair::*;

pub struct LsmTree<'a> {
    name: &'a str,
    log_file: File,
    tree: LogSegment<String>
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
            let existing_log = OpenOptions::new().read(true).write(true).append(true).open(log_file).unwrap();
            let tree = LsmTree{name, log_file: existing_log, tree: LogSegment::new()};
            let (tree, restore_result) = tree.restore();
            assert!(restore_result, "Failed to restore WAL!");
            return tree;
        }
        LsmTree{name, log_file: OpenOptions::new().create(true).write(true).append(true).open(log_file).unwrap(), tree: LogSegment::new()}
    }

    fn log<'b>(&mut self, entry: &'b KVPair) -> bool {
        match self.log_file.write(format!("{}\n", entry).as_bytes()) {
            Err(e) => {println!("failed to log {} to db {} with error {}", entry, self.name, e); false}
            Ok(_) => {println!("logged entry for {} for db {}", entry, self.name); true}
        }
    }

    fn restore(mut self) -> (LsmTree<'a>, bool) {
        let wal_contents = BufReader::new(self.log_file).lines();
        for line in wal_contents {
            if let Ok(line) = line {
                // WAL format is an append-only log of entries like below
                // key: foo, value: bar, we can re-create segment from log
                // by iterating these entries and applying as a sequence of writes
                self.tree = self.tree.insert(("a", "b"));
            }
        }
        let path_str = format!("{}.log", self.name);
        let log_file = Path::new(&path_str);
        self.log_file = OpenOptions::new().write(true).append(true).open(log_file).unwrap();
        (self, true)
    }

    pub fn write<'b>(mut self, key: &'a str, value: &'a str) -> (LsmTree<'a>, bool) {
        let kvp = KVPair::new(key, value);
        match self.log(&kvp) {
            true => {
                self.tree.insert((key.to_string(), value.to_string()));
                (self, true)
            },
            false => {(self, false)}
        }
    }

    pub fn get(&self, key: &'a str) -> Option<&String> {
        return self.tree.get(key.to_string());
    }
}