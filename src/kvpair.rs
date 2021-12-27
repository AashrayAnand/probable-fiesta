use std::{fmt::{Debug, Display, write}, cmp::Ordering};

#[derive(Debug, Clone, Copy)]
pub struct KVPair<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl KVPair<'_> {
    pub fn new<'a>(key: &'a str, value: &'a str) -> KVPair<'a> {
        KVPair{key, value}
    }
}

impl Ord for KVPair<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.key > other.key {
            Ordering::Greater
        }
        else if self.key < other.key {
            Ordering::Less
        }
        else {
            Ordering::Equal
        }
    }
}

impl Eq for KVPair<'_> {
}

impl PartialOrd for KVPair<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.value.partial_cmp(&other.value)
    }
}

impl Display for KVPair<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write(f, format_args!("key: {}, value: {}", self.key, self.value))
    }
}

impl PartialEq for KVPair<'_> {
    fn eq(&self, other: &KVPair) -> bool { 
        self.key == other.key
    }

    fn ne(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}