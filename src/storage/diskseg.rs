use std::{cmp::Ordering, fs::File};

use self::DiskSegment::*;

use super::tree::LogSegment;

pub enum DiskSegment {
    OpenSegment{path_s: String, file: File, tree: LogSegment<String>},
    ClosedSegment{path_s: String, file: File},
}

impl Ord for DiskSegment
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let own_path = match self {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let other_path = match other {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        own_path.cmp(&other_path)
    }
}

impl Eq for DiskSegment {
}

impl PartialEq for DiskSegment {
    fn eq(&self, other: &Self) -> bool {
        let own_path = match self {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let other_path = match other {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        own_path == other_path
    }
}

impl PartialOrd for DiskSegment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let own_path = match self {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let other_path = match other {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        own_path.partial_cmp(&other_path)
    }
}