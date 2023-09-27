use std::io;

use fnv::FnvHashMap;
use sstable::SSTable;

pub(crate) struct TermIdMapping {
    unordered_to_ord: Vec<OrderedId>,
}

impl TermIdMapping {
    pub fn to_ord(&self, unordered: UnorderedId) -> OrderedId {
        self.unordered_to_ord[unordered.0 as usize]
    }
}

/// When we add values, we cannot know their ordered id yet.
/// For this reason, we temporarily assign them a `UnorderedId`
/// that will be mapped to an `OrderedId` upon serialization.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct UnorderedId(pub u32);

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct OrderedId(pub u32);

/// `DictionaryBuilder` for dictionary encoding.
///
/// It stores the different terms encounterred and assigns them a temporary value
/// we call unordered id.
///
/// Upon serialization, we will sort the ids and hence build a `UnorderedId -> Term ordinal`
/// mapping.
#[derive(Default)]
pub(crate) struct DictionaryBuilder {
    dict: FnvHashMap<Vec<u8>, UnorderedId>,
    memory_consumption: usize,
}

impl DictionaryBuilder {
    /// Get or allocate an unordered id.
    /// (This ID is simply an auto-incremented id.)
    pub fn get_or_allocate_id(&mut self, term: &[u8]) -> UnorderedId {
        if let Some(term_id) = self.dict.get(term) {
            return *term_id;
        }
        let new_id = UnorderedId(self.dict.len() as u32);
        self.dict.insert(term.to_vec(), new_id);
        self.memory_consumption += term.len();
        self.memory_consumption += 40; // Term Metadata + HashMap overhead
        new_id
    }

    /// Serialize the dictionary into an fst, and returns the
    /// `UnorderedId -> TermOrdinal` map.
    pub fn serialize<'a, W: io::Write + 'a>(&self, wrt: &mut W) -> io::Result<TermIdMapping> {
        let mut terms: Vec<(&[u8], UnorderedId)> =
            self.dict.iter().map(|(k, v)| (k.as_slice(), *v)).collect();
        terms.sort_unstable_by_key(|(key, _)| *key);
        // TODO Remove the allocation.
        let mut unordered_to_ord: Vec<OrderedId> = vec![OrderedId(0u32); terms.len()];
        let mut sstable_builder = sstable::VoidSSTable::writer(wrt);
        for (ord, (key, unordered_id)) in terms.into_iter().enumerate() {
            let ordered_id = OrderedId(ord as u32);
            sstable_builder.insert(key, &())?;
            unordered_to_ord[unordered_id.0 as usize] = ordered_id;
        }
        sstable_builder.finish()?;
        Ok(TermIdMapping { unordered_to_ord })
    }

    pub(crate) fn mem_usage(&self) -> usize {
        self.memory_consumption
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_builder() {
        let mut dictionary_builder = DictionaryBuilder::default();
        let hello_uid = dictionary_builder.get_or_allocate_id(b"hello");
        let happy_uid = dictionary_builder.get_or_allocate_id(b"happy");
        let tax_uid = dictionary_builder.get_or_allocate_id(b"tax");
        let mut buffer = Vec::new();
        let id_mapping = dictionary_builder.serialize(&mut buffer).unwrap();
        assert_eq!(id_mapping.to_ord(hello_uid), OrderedId(1));
        assert_eq!(id_mapping.to_ord(happy_uid), OrderedId(0));
        assert_eq!(id_mapping.to_ord(tax_uid), OrderedId(2));
    }
}
