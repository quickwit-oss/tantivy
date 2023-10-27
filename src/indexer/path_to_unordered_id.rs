use fnv::FnvHashMap;

#[derive(Default)]
pub(crate) struct PathToUnorderedId {
    map: FnvHashMap<String, u32>,
}

impl PathToUnorderedId {
    #[inline]
    pub(crate) fn get_id(&mut self, path: &str) -> u32 {
        if let Some(id) = self.map.get(path) {
            return *id;
        }
        self.insert_new_path(path)
    }
    #[cold]
    fn insert_new_path(&mut self, path: &str) -> u32 {
        let next_id = self.map.len() as u32;
        self.map.insert(path.to_string(), next_id);
        next_id
    }

    /// Retuns ids which reflect the lexical order of the paths.
    ///
    /// The returned vec can be indexed with the unordered id to get the ordered id.
    pub(crate) fn unordered_id_to_ordered_id(&self) -> Vec<u32> {
        let mut sorted_ids: Vec<(&String, &u32)> = self.map.iter().collect();
        sorted_ids.sort_unstable_by_key(|(path, _)| *path);
        let mut result = vec![sorted_ids.len() as u32; sorted_ids.len()];
        for (ordered, unordered) in sorted_ids.iter().map(|(_k, v)| v).enumerate() {
            result[**unordered as usize] = ordered as u32;
        }
        result
    }

    /// Retuns the paths so they can be queried by the ordered id (which is the index).
    pub(crate) fn ordered_id_to_path(&self) -> Vec<&String> {
        let mut paths = self.map.keys().collect::<Vec<_>>();
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
            .map(|term| path_to_id.get_id(term))
            .collect::<Vec<u32>>();
        assert_eq!(ids, vec![0, 1, 0, 2]);
        let ordered_ids = ids
            .iter()
            .map(|id| path_to_id.unordered_id_to_ordered_id()[*id as usize])
            .collect::<Vec<u32>>();
        assert_eq!(ordered_ids, vec![1, 0, 1, 2]);
        // Fetch terms
        let terms_fetched = ordered_ids
            .iter()
            .map(|id| path_to_id.ordered_id_to_path()[*id as usize])
            .collect::<Vec<&String>>();
        assert_eq!(terms_fetched, terms);
    }
}
