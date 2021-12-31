use std::fmt::{Debug, Display};
use std::{cmp::*};
use std::fs::File;
use std::io::{Write};
use crate::storage::tree::LogSegment::*;

pub enum LogSegment<T: Ord + Debug + Display> {
    TreeNode{k: T, v: Option<T>, left: Box<LogSegment<T>>, right: Box<LogSegment<T>>},
    Nil
}

impl<T: Ord + Debug + Display> LogSegment<T> {
    pub fn new() -> LogSegment<T> {
        Nil
    }

    pub fn insert(&mut self, pair: (T, T)) {
        match self {
            Nil => {
                *self = TreeNode { k: pair.0, v: Some(pair.1), left: Box::new(Nil), right: Box::new(Nil) };
            },
            TreeNode{k, v, left, right} => {
                match pair.0.cmp(k) {
                    Ordering::Equal => *v = Some(pair.1),
                    Ordering::Greater => right.insert(pair),
                    Ordering::Less => left.insert(pair),
                }
            }
        }
    }

    pub fn delete(&mut self, del_key: T) {
        match self {
            Nil => {},
            TreeNode { k, v, left, right } => {
                match del_key.cmp(k) {
                    Ordering::Equal => *v = None,
                    Ordering::Greater => right.delete(del_key),
                    Ordering::Less => left.delete(del_key)
                }
            }
        }
    }

    pub fn get(&self, get_key: T) -> Option<&T> {
        match self {
            Nil => {None},
            TreeNode { k, v, left, right } => {
                match get_key.cmp(k) {
                    Ordering::Equal => v.as_ref(),
                    Ordering::Greater => right.get(get_key),
                    Ordering::Less => left.get(get_key),
                }
            }
        }
    }

    // in-order traversal and write to disk of the tree.
    pub fn write_to_disk(&mut self, file: &mut File) {
        match self {
            Nil => {},
            TreeNode { k, v, left, right } => {
                left.write_to_disk(file);

                if let Some(v) = v {
                    if let Err(e) = file.write(format!("{} {}\n", k, v).as_bytes()) {
                        panic!("{}", e);
                    }
                }
                else {
                    // Tombstoned entries will be written to disk as keys only
                    if let Err(e) = file.write(format!("{}\n", k).as_bytes()) {
                        panic!("{}", e);
                    }
                }
                right.write_to_disk(file);
            }
        }
    }

    pub fn exists(&self, ex_key: T) -> bool {
        match self.get(ex_key) {
            Some(_) => true,
            _ => false
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Nil => {
                0
            }
            TreeNode { k: _, v, left, right } => {
                // Values are tombstoned in place rather than being deleted from
                // the tree, so we need to check if there a valid sum type before
                // we increment the size count
                match v {
                    Some(_) => {1 + left.size() + right.size()},
                    None => {left.size() + right.size()}
                }
            }
        }
    }
}