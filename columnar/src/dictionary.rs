use std::io;

use fnv::FnvHashMap;

fn fst_err_into_io_err(fst_err: tantivy_fst::Error) -> io::Error {
    match fst_err {
        tantivy_fst::Error::Fst(fst_err) => {
            io::Error::new(io::ErrorKind::Other, format!("FST Error: {:?}", fst_err))
        }
        tantivy_fst::Error::Io(io_err) => io_err,
    }
}

/// `DictionaryBuilder` for dictionary encoding.
///
/// It stores the different terms encounterred and assigns them a temporary value
/// we call unordered id.
///
/// Upon serialization, we will sort the ids and hence build a `UnorderedId -> Term ordinal`
/// mapping.
#[derive(Default)]
pub struct DictionaryBuilder {
    dict: FnvHashMap<Vec<u8>, UnorderedId>,
}

pub struct IdMapping {
    unordered_to_ord: Vec<OrderedId>,
}

impl IdMapping {
    pub fn to_ord(&self, unordered: UnorderedId) -> OrderedId {
        self.unordered_to_ord[unordered.0 as usize]
    }
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
        new_id
    }

    /// Serialize the dictionary into an fst, and returns the
    /// `UnorderedId -> TermOrdinal` map.
    pub fn serialize<'a, W: io::Write + 'a>(&self, wrt: &mut W) -> io::Result<IdMapping> {
        serialize_inner(&self.dict, wrt).map_err(fst_err_into_io_err)
    }
}

/// Helper function just there for error conversion.
fn serialize_inner<'a, W: io::Write + 'a>(
    dict: &FnvHashMap<Vec<u8>, UnorderedId>,
    wrt: &mut W,
) -> tantivy_fst::Result<IdMapping> {
    let mut terms: Vec<(&[u8], UnorderedId)> =
        dict.iter().map(|(k, v)| (k.as_slice(), *v)).collect();
    terms.sort_unstable_by_key(|(key, _)| *key);
    let mut unordered_to_ord: Vec<OrderedId> = vec![OrderedId(0u32); terms.len()];
    let mut fst_builder = tantivy_fst::MapBuilder::new(wrt)?;
    for (ord, (key, unordered_id)) in terms.into_iter().enumerate() {
        let ordered_id = OrderedId(ord as u32);
        fst_builder.insert(key, ord as u64)?;
        unordered_to_ord[unordered_id.0 as usize] = ordered_id;
    }
    fst_builder.finish()?;
    Ok(IdMapping { unordered_to_ord })
}

#[derive(Clone, Copy, Debug)]
pub struct UnorderedId(pub u32);

#[derive(Clone, Copy)]
pub struct OrderedId(pub u32);
