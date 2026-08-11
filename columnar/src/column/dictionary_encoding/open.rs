use std::io;
use std::sync::Arc;

use common::OwnedBytes;
use sstable::Dictionary;

use super::DictionaryEncodedBytesColumn;
use crate::Version;

pub(crate) fn open_dictionary_bytes_column(
    data: OwnedBytes,
    format_version: Version,
) -> io::Result<DictionaryEncodedBytesColumn> {
    if data.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated dictionary string/byte column payload",
        ));
    }
    let (body, dictionary_len_bytes) = data.rsplit(4);
    let dictionary_len = u32::from_le_bytes(dictionary_len_bytes.as_slice().try_into().unwrap());
    if dictionary_len as usize > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dictionary length exceeds string/byte column payload",
        ));
    }
    let (dictionary_bytes, column_bytes) = body.split(dictionary_len as usize);
    let dictionary = Arc::new(Dictionary::from_bytes(dictionary_bytes)?);
    let term_ord_column = crate::column::open_column_u64::<u64>(column_bytes, format_version)?;
    Ok(DictionaryEncodedBytesColumn {
        dictionary,
        term_ord_column,
    })
}
