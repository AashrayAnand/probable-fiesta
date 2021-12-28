#[cfg(test)]
use crate::storage::tree::LogSegment;

#[test]
pub fn test_bst_insert() {
    let mut tree: LogSegment<&str> = LogSegment::new();

    let first_ten_letters = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];
    let mut exp_size = 0;
    for i in 0..first_ten_letters.len() {
        tree = tree.insert((first_ten_letters[i], first_ten_letters[i]));
        exp_size += 1;
        assert!(tree.size() == exp_size, "Expected tree size {}, actually is {}", exp_size, tree.size());

    }
}

#[test]
pub fn test_bst_insert_delete() {
    let mut tree: LogSegment<&str> = LogSegment::new();

    let first_ten_letters = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];
    let mut exp_size = 0;
    for i in 0..first_ten_letters.len() {
        tree = tree.insert((first_ten_letters[i], first_ten_letters[i]));
        exp_size += 1;
        assert!(tree.size() == exp_size, "Expected tree size {}, actually is {}", exp_size, tree.size());
        assert!(tree.exists(first_ten_letters[i]), "The letter {} doesn't exist in the tree after insert", first_ten_letters[i]);
    }

    for i in 0..first_ten_letters.len() {
        tree = tree.delete(first_ten_letters[i]);
        exp_size -= 1;
        assert!(tree.size() == exp_size, "Expected tree size {}, actually is {}", exp_size, tree.size());
        assert!(!tree.exists(first_ten_letters[i]), "The letter {} exists in the tree after delete", first_ten_letters[i]);
    }
}