use columnar::ColumnType;
use common::TinySet;
use fnv::FnvHashMap;

/// `Field` is represented by an unsigned 32-bit integer type.
/// The schema holds the mapping between field names and `Field` objects.
#[derive(Copy, Default, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct OrderedPathId(u32);

impl OrderedPathId {
    /// Create a new field object for the given PathId.
    pub const fn from_ordered_id(field_id: u32) -> OrderedPathId {
        OrderedPathId(field_id)
    }

    /// Returns a u32 identifying uniquely a path within a schema.
    pub const fn path_id(self) -> u32 {
        self.0
    }
}
impl From<u32> for OrderedPathId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

#[derive(Default)]
pub(crate) struct PathToUnorderedId {
    /// TinySet contains the type codes of the columns in the path.
    map: FnvHashMap<String, (u32, TinySet)>,
}

impl PathToUnorderedId {
    #[inline]
    pub(crate) fn get_or_allocate_unordered_id(&mut self, path: &str, typ: ColumnType) -> u32 {
        let code_bit = typ.to_code();
        if let Some((id, all_codes)) = self.map.get_mut(path) {
            *all_codes = all_codes.insert(code_bit as u32);
            return *id;
        }
        self.insert_new_path(path, code_bit)
    }
    #[cold]
    fn insert_new_path(&mut self, path: &str, typ_code: u8) -> u32 {
        let next_id = self.map.len() as u32;
        self.map.insert(
            path.to_string(),
            (next_id, TinySet::singleton(typ_code as u32)),
        );
        next_id
    }

    /// Retuns ids which reflect the lexical order of the paths.
    ///
    /// The returned vec can be indexed with the unordered id to get the ordered id.
    pub(crate) fn unordered_id_to_ordered_id(&self) -> Vec<(OrderedPathId, TinySet)> {
        let mut sorted_ids: Vec<(&str, (u32, TinySet))> = self
            .map
            .iter()
            .map(|(k, (id, typ_code))| (k.as_str(), (*id, *typ_code)))
            .collect();
        sorted_ids.sort_unstable_by_key(|(path, _)| *path);
        let mut result = vec![(OrderedPathId::default(), TinySet::empty()); sorted_ids.len()];
        for (ordered, (unordered, typ_code)) in sorted_ids.iter().map(|(_k, v)| v).enumerate() {
            result[*unordered as usize] =
                (OrderedPathId::from_ordered_id(ordered as u32), *typ_code);
        }
        result
    }

    /// Retuns the paths so they can be queried by the ordered id (which is the index).
    pub(crate) fn ordered_id_to_path(&self) -> Vec<&str> {
        let mut paths = self.map.keys().map(String::as_str).collect::<Vec<_>>();
        paths.sort_unstable();
        paths
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_to_unordered_test() {
        let mut path_to_id = PathToUnorderedId::default();
        let terms = vec!["b", "a", "b", "c"];
        let ids = terms
            .iter()
            .map(|term| path_to_id.get_or_allocate_unordered_id(term, ColumnType::Str))
            .collect::<Vec<u32>>();
        assert_eq!(ids, vec![0, 1, 0, 2]);
        let ordered_ids = ids
            .iter()
            .map(|id| path_to_id.unordered_id_to_ordered_id()[*id as usize].0)
            .collect::<Vec<OrderedPathId>>();
        assert_eq!(ordered_ids, vec![1.into(), 0.into(), 1.into(), 2.into()]);
        // Fetch terms
        let terms_fetched = ordered_ids
            .iter()
            .map(|id| path_to_id.ordered_id_to_path()[id.path_id() as usize])
            .collect::<Vec<&str>>();
        assert_eq!(terms_fetched, terms);
    }
}
