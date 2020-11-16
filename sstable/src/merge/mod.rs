mod fast_merge;
mod heap_merge;

pub use self::fast_merge::merge_sstable;
pub use self::heap_merge::merge_sstable as merge_sstable_heap;


pub trait SingleValueMerger<V> {
    fn add(&mut self, v: &V);
    fn finish(self) -> V;
}

pub trait ValueMerger<V> {
    type TSingleValueMerger: SingleValueMerger<V>;
    fn new_value(&mut self, v: &V) -> Self::TSingleValueMerger;
}

#[derive(Default)]
pub struct KeepFirst;

pub struct FirstVal<V>(V);

impl<V: Clone> ValueMerger<V> for KeepFirst {
    type TSingleValueMerger = FirstVal<V>;

    fn new_value(&mut self, v: &V) -> FirstVal<V> {
        FirstVal(v.clone())
    }
}

impl<V> SingleValueMerger<V> for FirstVal<V> {
    fn add(&mut self, _: &V) {}

    fn finish(self) -> V {
        self.0
    }
}

pub struct VoidMerge;
impl ValueMerger<()> for VoidMerge {

    type TSingleValueMerger = ();

    fn new_value(&mut self, _: &()) -> () {
        ()
    }
}

impl SingleValueMerger<()> for () {
    fn add(&mut self, _: &()) {}

    fn finish(self) -> () {
        ()
    }
}

#[cfg(test)]
mod tests {

    use VoidSSTable;
    use SSTable;
    use super::VoidMerge;
    use std::str;
    use std::collections::BTreeSet;

    fn write_sstable(keys: &[&'static str]) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            for &key in keys {
                assert!(sstable_writer.write(key.as_bytes(), &()).is_ok());
            }
            assert!(sstable_writer.finalize().is_ok());
        }
        buffer
    }

    fn merge_test_aux(arrs: &[&[&'static str]]) {
        let sstables = arrs.iter()
            .cloned()
            .map(write_sstable)
            .collect::<Vec<_>>();
        let sstables_ref: Vec<&[u8]> = sstables.iter()
            .map(|s| s.as_ref())
            .collect();
        let mut merged = BTreeSet::new();
        for &arr in arrs.iter() {
            for &s in arr {
                merged.insert(s.to_string());
            }
        }
        let mut w = Vec::new();
        assert!(VoidSSTable::merge(sstables_ref, &mut w, VoidMerge).is_ok());
    }

    #[test]
    fn test_merge() {
        merge_test_aux(&[]);
        merge_test_aux(&[&["a"]]);
        merge_test_aux(&[&["a","b"], &["ab"]]); // a, ab, b
        merge_test_aux(&[&["a","b"], &["a", "b"]]);
        merge_test_aux(&[
            &["happy", "hello",  "payer", "tax"],
            &["habitat", "hello", "zoo"],
            &[],
            &["a"],
        ]);
        merge_test_aux(&[&["a"]]);
        merge_test_aux(&[&["a","b"], &["ab"]]);
        merge_test_aux(&[&["a","b"], &["a", "b"]]);
    }
}