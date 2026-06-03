use rustc_hash::FxHashMap;

use crate::aggregation::intermediate_agg_result::IntermediateKey;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StringRef {
    start: u32,
    len: u32,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StringArena {
    buffer: String,
}

impl StringArena {
    pub fn register_str(&mut self, value: &str) -> StringRef {
        let start = self.buffer.len() as u32;
        self.buffer.push_str(value);
        StringRef {
            start,
            len: value.len() as u32,
        }
    }

    pub fn get_str(&self, string_ref: StringRef) -> &str {
        let start = string_ref.start as usize;
        let end = start + string_ref.len as usize;
        &self.buffer[start..end]
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

pub(crate) struct TermOrdToStrCache {
    string_arena: StringArena,
    missing_key: Option<IntermediateKey>,
    addr: TermOrdToAddr,
}

enum TermOrdToAddr {
    Dense { offsets: Vec<Option<StringRef>> },
    Sparse { terms: FxHashMap<u64, StringRef> },
}

impl std::fmt::Debug for TermOrdToStrCache {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.addr {
            TermOrdToAddr::Dense { offsets } => f
                .debug_struct("TermOrdToStrCache::Dense")
                .field("num_slots", &offsets.len())
                .field("arena_bytes", &self.string_arena.len())
                .finish(),
            TermOrdToAddr::Sparse { terms } => f
                .debug_struct("TermOrdToStrCache::Sparse")
                .field("num_terms", &terms.len())
                .field("arena_bytes", &self.string_arena.len())
                .finish(),
        }
    }
}

impl TermOrdToStrCache {
    /// `term_ords` must be sorted. Each entry in `string_refs` is a reference
    /// into `string_arena` for the corresponding term ord.
    pub fn new(
        term_ords: Vec<u64>,
        string_refs: Vec<StringRef>,
        string_arena: StringArena,
        missing_key: Option<IntermediateKey>,
    ) -> TermOrdToStrCache {
        let num_terms = term_ords.len();
        assert_eq!(num_terms, string_refs.len());

        if term_ords.is_empty() {
            return TermOrdToStrCache {
                string_arena,
                missing_key,
                addr: TermOrdToAddr::Dense {
                    offsets: Vec::new(),
                },
            };
        }

        let highest_term_ord = term_ords.last().copied().unwrap_or(0u64);
        let should_use_dense =
            highest_term_ord < 1_000_000u64 || highest_term_ord < num_terms as u64 * 3u64;

        let addr = if should_use_dense {
            let num_slots = highest_term_ord as usize + 1;
            let mut offsets: Vec<Option<StringRef>> = vec![None; num_slots];
            for (term_ord, string_ref) in term_ords.into_iter().zip(string_refs) {
                offsets[term_ord as usize] = Some(string_ref);
            }
            TermOrdToAddr::Dense { offsets }
        } else {
            let terms: FxHashMap<u64, StringRef> = term_ords.into_iter().zip(string_refs).collect();
            TermOrdToAddr::Sparse { terms }
        };

        TermOrdToStrCache {
            string_arena,
            missing_key,
            addr,
        }
    }

    pub fn get(&self, term_ord: u64) -> Option<&str> {
        let string_ref = match &self.addr {
            TermOrdToAddr::Dense { offsets } => (*offsets.get(term_ord as usize)?)?,
            TermOrdToAddr::Sparse { terms } => *terms.get(&term_ord)?,
        };
        Some(self.string_arena.get_str(string_ref))
    }

    pub fn missing_key(&self) -> Option<&IntermediateKey> {
        self.missing_key.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_arena() {
        let mut arena = StringArena::default();
        let r1 = arena.register_str("hello");
        let r2 = arena.register_str("world");
        let r3 = arena.register_str("");
        let r4 = arena.register_str("!");

        assert_eq!(arena.get_str(r1), "hello");
        assert_eq!(arena.get_str(r2), "world");
        assert_eq!(arena.get_str(r3), "");
        assert_eq!(arena.get_str(r4), "!");
    }

    fn build_arena(terms: &[&str]) -> (StringArena, Vec<StringRef>) {
        let mut arena = StringArena::default();
        let refs: Vec<StringRef> = terms.iter().map(|t| arena.register_str(t)).collect();
        (arena, refs)
    }

    #[test]
    fn test_dense_cache() {
        let term_ords = vec![0, 2, 5];
        let (arena, refs) = build_arena(&["alpha", "beta", "gamma"]);
        let cache = TermOrdToStrCache::new(term_ords, refs, arena, None);

        assert_eq!(cache.get(0), Some("alpha"));
        assert_eq!(cache.get(1), None);
        assert_eq!(cache.get(2), Some("beta"));
        assert_eq!(cache.get(3), None);
        assert_eq!(cache.get(5), Some("gamma"));
        assert_eq!(cache.get(6), None);
        assert_eq!(cache.get(100), None);
    }

    #[test]
    fn test_sparse_cache() {
        let term_ords = vec![1_000_000, 2_000_000, 5_000_000];
        let (arena, refs) = build_arena(&["foo", "bar", "baz"]);
        let cache = TermOrdToStrCache::new(term_ords, refs, arena, None);

        assert_eq!(cache.get(1_000_000), Some("foo"));
        assert_eq!(cache.get(2_000_000), Some("bar"));
        assert_eq!(cache.get(5_000_000), Some("baz"));
        assert_eq!(cache.get(0), None);
        assert_eq!(cache.get(3_000_000), None);
    }

    #[test]
    fn test_empty_cache() {
        let cache = TermOrdToStrCache::new(Vec::new(), Vec::new(), StringArena::default(), None);
        assert_eq!(cache.get(0), None);
        assert_eq!(cache.get(100), None);
    }

    #[test]
    fn test_missing_key() {
        let missing = IntermediateKey::Str("N/A".to_string());
        let (arena, refs) = build_arena(&["x"]);
        let cache = TermOrdToStrCache::new(vec![0], refs, arena, Some(missing));
        assert_eq!(
            cache.missing_key(),
            Some(&IntermediateKey::Str("N/A".to_string()))
        );

        let (arena, refs) = build_arena(&["x"]);
        let cache_no_missing = TermOrdToStrCache::new(vec![0], refs, arena, None);
        assert_eq!(cache_no_missing.missing_key(), None);
    }
}
