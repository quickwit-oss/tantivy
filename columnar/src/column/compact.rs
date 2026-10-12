//! Independently decodable OnPair16 row groups.
//!
//! Integers on disk are little-endian. A column contains a storage tag (written by
//! ColumnarSerializer), groups, a directory, and a 64-byte footer. Directory
//! entries are (document start: u32, value start: u32, byte offset: u64), including
//! a final sentinel. Value starts let ColumnIndex expose global value row IDs
//! without loading preceding groups. These IDs are positions, never term ordinals.
//! Each group contains a 64-byte header, a local document index, a padded symbol
//! dictionary, value-to-code offsets, and u16 codes. No buffer borrows unaligned
//! typed data from a file mapping.

use std::io::{self, Write};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use common::OwnedBytes;
use common::file_slice::FileSlice;
use onpair::{CompactDictionary, Dictionary, OwnedDictionaryStorage};

use crate::{Cardinality, CompactCodecOptions, DocId, RowId};

const HEADER_LEN: usize = 64;
const FOOTER_LEN: usize = 64;
const DIRECTORY_ENTRY_LEN: usize = 16;
const MAGIC: &[u8; 4] = b"CPC1";

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn input_error(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn read_array<const LEN: usize>(bytes: &[u8], position: usize) -> io::Result<[u8; LEN]> {
    let end = position
        .checked_add(LEN)
        .ok_or_else(|| invalid("compact offset overflow"))?;
    bytes
        .get(position..end)
        .ok_or_else(|| invalid("truncated compact column"))?
        .try_into()
        .map_err(|_| invalid("invalid compact integer"))
}

fn read_u32(bytes: &[u8], position: usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(read_array(bytes, position)?))
}

fn read_u64(bytes: &[u8], position: usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(read_array(bytes, position)?))
}

fn as_usize(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid("compact offset exceeds address space"))
}

#[derive(Clone, Copy, Debug)]
struct GroupAddress {
    doc: DocId,
    value: RowId,
    offset: u64,
}

#[derive(Debug)]
struct CompactData {
    file: FileSlice,
    directory: Vec<GroupAddress>,
    cardinality: Cardinality,
    // A single validated group is retained. Clones share it; callers keep an Arc
    // while decoding, so another reader cannot invalidate their group buffers.
    cache: Mutex<Option<(usize, Arc<Group>)>>,
}

/// A compact column and its lazy document-to-group index.
#[derive(Clone, Debug)]
pub struct CompactColumn(Arc<CompactData>);

#[derive(Debug)]
struct Group {
    // Full and empty groups have implicit indexes. Other groups store one
    // cumulative value offset per document plus an end sentinel.
    doc_offsets: Vec<u32>,
    cardinality: u8,
    dictionary: Option<CompactDictionary>,
    value_offsets: Vec<u64>,
    codes: Vec<u16>,
}

impl CompactColumn {
    /// Opens metadata only. The first byte of `file` is the compact storage tag.
    pub(crate) fn open(file: FileSlice) -> io::Result<Self> {
        let length = file.num_bytes().get_bytes() as usize;
        if length < 1 + FOOTER_LEN + DIRECTORY_ENTRY_LEN {
            return Err(invalid("truncated compact footer"));
        }
        let footer = file.slice(length - FOOTER_LEN..length).read_bytes()?;
        if &footer[..4] != MAGIC
            || u16::from_le_bytes(read_array(&footer, 4)?) != 1
            || read_array::<2>(&footer, 6)? != [0; 2]
            || footer[53..].iter().any(|&byte| byte != 0)
        {
            return Err(invalid("unsupported compact format"));
        }
        let num_docs = read_u32(&footer, 8)?;
        let num_groups = read_u32(&footer, 12)? as usize;
        let num_values = u32::try_from(read_u64(&footer, 16)?)
            .map_err(|_| invalid("too many compact values"))?;
        let directory_offset = as_usize(read_u64(&footer, 24)?)?;
        let directory_len = as_usize(read_u64(&footer, 32)?)?;
        let expected_len = num_groups
            .checked_add(1)
            .and_then(|count| count.checked_mul(DIRECTORY_ENTRY_LEN))
            .ok_or_else(|| invalid("compact directory overflow"))?;
        if directory_len != expected_len
            || directory_offset < 1
            || directory_offset.checked_add(directory_len) != Some(length - FOOTER_LEN)
        {
            return Err(invalid("invalid compact directory extent"));
        }
        let cardinality = Cardinality::try_from_code(footer[52])?;
        let bytes = file
            .slice(directory_offset..length - FOOTER_LEN)
            .read_bytes()?;
        if crc32fast::hash(&bytes) != read_u32(&footer, 48)? {
            return Err(invalid("compact directory checksum mismatch"));
        }
        let mut directory = Vec::with_capacity(num_groups + 1);
        for entry in bytes.as_chunks::<DIRECTORY_ENTRY_LEN>().0 {
            directory.push(GroupAddress {
                doc: read_u32(entry, 0)?,
                value: read_u32(entry, 4)?,
                offset: read_u64(entry, 8)?,
            });
        }
        let first = directory[0];
        let last = directory[num_groups];
        if first.doc != 0
            || first.value != 0
            || first.offset != 1
            || last.doc != num_docs
            || last.value != num_values
            || last.offset != directory_offset as u64
        {
            return Err(invalid("invalid compact directory sentinels"));
        }
        for pair in directory.windows(2) {
            if pair[0].doc >= pair[1].doc
                || pair[0].value > pair[1].value
                || pair[0]
                    .offset
                    .checked_add(HEADER_LEN as u64)
                    .is_none_or(|end| end > pair[1].offset)
            {
                return Err(invalid("invalid compact group range"));
            }
        }
        Ok(Self(Arc::new(CompactData {
            file,
            directory,
            cardinality,
            cache: Mutex::new(None),
        })))
    }

    pub fn num_docs(&self) -> u32 {
        self.0.directory.last().unwrap().doc
    }
    pub fn num_values(&self) -> u32 {
        self.0.directory.last().unwrap().value
    }
    pub fn cardinality(&self) -> Cardinality {
        self.0.cardinality
    }
    pub fn num_groups(&self) -> usize {
        self.0.directory.len() - 1
    }

    /// Returns the byte range of a document's group, relative to its column.
    pub fn group_range(&self, doc: DocId) -> io::Result<Range<u64>> {
        let group = self.group_for_doc(doc)?;
        Ok(self.0.directory[group].offset..self.0.directory[group + 1].offset)
    }

    fn group_for_doc(&self, doc: DocId) -> io::Result<usize> {
        if doc >= self.num_docs() {
            return Err(input_error("compact document out of range"));
        }
        Ok(self.0.directory.partition_point(|entry| entry.doc <= doc) - 1)
    }

    fn group(&self, group_id: usize) -> io::Result<Arc<Group>> {
        {
            let cache = self.0.cache.lock().unwrap();
            if let Some((cached_id, group)) = cache.as_ref()
                && *cached_id == group_id
            {
                return Ok(group.clone());
            }
        }
        let address = self.0.directory[group_id];
        let end = self.0.directory[group_id + 1];
        let bytes = self
            .0
            .file
            .slice(as_usize(address.offset)?..as_usize(end.offset)?)
            .read_bytes()?;
        let group = Arc::new(Group::open(
            bytes,
            end.doc - address.doc,
            end.value - address.value,
        )?);
        let cardinality = match group.cardinality {
            0 | 2 => Cardinality::Optional,
            1 => Cardinality::Full,
            _ => Cardinality::Multivalued,
        };
        if cardinality > self.cardinality() {
            return Err(invalid(
                "compact column cardinality understates group cardinality",
            ));
        }
        *self.0.cache.lock().unwrap() = Some((group_id, group.clone()));
        Ok(group)
    }

    /// Global value positions for a document. Positions are not dictionary IDs.
    pub fn value_range(&self, doc: DocId) -> io::Result<Range<RowId>> {
        let group_id = self.group_for_doc(doc)?;
        let address = self.0.directory[group_id];
        let group = self.group(group_id)?;
        let range = group.value_range(doc - address.doc);
        Ok(address.value + range.start..address.value + range.end)
    }

    pub fn value_rank(&self, doc: DocId) -> io::Result<RowId> {
        if doc == self.num_docs() {
            return Ok(self.num_values());
        }
        Ok(self.value_range(doc)?.start)
    }

    /// Decodes one value into a reusable buffer, replacing its previous contents.
    pub fn read_value(&self, value: RowId, output: &mut Vec<u8>) -> io::Result<()> {
        output.clear();
        if value >= self.num_values() {
            return Err(input_error("compact value out of range"));
        }
        let group_id = self
            .0
            .directory
            .partition_point(|entry| entry.value <= value)
            - 1;
        let group = self.group(group_id)?;
        group.decode(value - self.0.directory[group_id].value, output)
    }

    pub fn first(&self, doc: DocId, output: &mut Vec<u8>) -> io::Result<bool> {
        output.clear();
        let range = self.value_range(doc)?;
        if range.is_empty() {
            return Ok(false);
        }
        self.read_value(range.start, output)?;
        Ok(true)
    }

    pub fn next_non_null_doc(&self, mut doc: DocId) -> io::Result<Option<DocId>> {
        while doc < self.num_docs() {
            let group_id = self.group_for_doc(doc)?;
            let address = self.0.directory[group_id];
            let end_doc = self.0.directory[group_id + 1].doc;
            let group = self.group(group_id)?;
            if group.cardinality == 0 {
                doc = end_doc;
                continue;
            }
            while doc < end_doc {
                if !group.value_range(doc - address.doc).is_empty() {
                    return Ok(Some(doc));
                }
                doc += 1;
            }
        }
        Ok(None)
    }

    pub fn doc_for_value(&self, value: RowId) -> io::Result<DocId> {
        if value >= self.num_values() {
            return Err(input_error("compact value out of range"));
        }
        let group_id = self
            .0
            .directory
            .partition_point(|entry| entry.value <= value)
            - 1;
        let address = self.0.directory[group_id];
        let group = self.group(group_id)?;
        let local_value = value - address.value;
        let local_doc = if group.cardinality == 1 {
            local_value
        } else {
            (group
                .doc_offsets
                .partition_point(|&offset| offset <= local_value)
                - 1) as u32
        };
        Ok(address.doc + local_doc)
    }
}

impl Group {
    fn value_range(&self, doc: u32) -> Range<u32> {
        match self.cardinality {
            0 => 0..0,
            1 => doc..doc + 1,
            _ => self.doc_offsets[doc as usize]..self.doc_offsets[doc as usize + 1],
        }
    }
    fn open(bytes: OwnedBytes, num_docs: u32, num_values: u32) -> io::Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(invalid("truncated compact group"));
        }
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[..48]);
        hasher.update(&[0; 4]);
        hasher.update(&bytes[52..]);
        if hasher.finalize() != read_u32(&bytes, 48)? {
            return Err(invalid("compact group checksum mismatch"));
        }
        if u16::from_le_bytes(read_array(&bytes, 0)?) != 1
            || bytes[52..64].iter().any(|&byte| byte != 0)
            || read_u32(&bytes, 4)? != num_docs
            || read_u32(&bytes, 8)? != num_values
        {
            return Err(invalid("invalid compact group header"));
        }
        let cardinality = bytes[2];
        let width = bytes[3] as usize;
        if cardinality > 3 || !matches!(width, 4 | 8) {
            return Err(invalid("invalid compact group encoding"));
        }
        let num_symbols = read_u32(&bytes, 12)? as usize;
        let num_codes = as_usize(read_u64(&bytes, 16)?)?;
        let decoded_bytes = read_u64(&bytes, 24)?;
        let dict_bytes_len = read_u32(&bytes, 32)? as usize;
        let index_len = read_u32(&bytes, 36)? as usize;
        let max_value_len = read_u64(&bytes, 40)?;
        if num_symbols > 65536 {
            return Err(invalid("too many OnPair symbols"));
        }
        let symbol_offsets_len = if num_symbols == 0 {
            0
        } else {
            (num_symbols + 1) * 4
        };
        let value_offsets_len = (num_values as usize)
            .checked_add(1)
            .and_then(|count| count.checked_mul(width))
            .ok_or_else(|| invalid("compact value offsets overflow"))?;
        let total_len = HEADER_LEN
            .checked_add(index_len)
            .and_then(|end| end.checked_add(symbol_offsets_len))
            .and_then(|end| end.checked_add(dict_bytes_len))
            .and_then(|end| end.checked_add(value_offsets_len))
            .and_then(|end| {
                num_codes
                    .checked_mul(2)
                    .and_then(|size| end.checked_add(size))
            });
        if total_len != Some(bytes.len()) {
            return Err(invalid("invalid compact group sections"));
        }
        let mut position = HEADER_LEN;
        let mut doc_offsets = Vec::new();
        if cardinality == 0 {
            if num_values != 0
                || num_symbols != 0
                || num_codes != 0
                || index_len != 0
                || dict_bytes_len != 0
            {
                return Err(invalid("invalid empty compact group"));
            }
            // No local index or symbol dictionary is needed for an empty group.
        } else {
            if num_values == 0 || num_symbols < 256 {
                return Err(invalid("invalid nonempty compact group"));
            }
            if cardinality == 1 {
                if num_values != num_docs || index_len != 0 {
                    return Err(invalid("invalid full compact index"));
                }
            } else {
                let expected_index_len = (num_docs as usize)
                    .checked_add(1)
                    .and_then(|count| count.checked_mul(4));
                if expected_index_len != Some(index_len) {
                    return Err(invalid("invalid compact index length"));
                }
                doc_offsets = Vec::with_capacity(num_docs as usize + 1);
                for entry in bytes[position..position + index_len].as_chunks::<4>().0 {
                    doc_offsets.push(read_u32(entry, 0)?);
                }
                if doc_offsets.first() != Some(&0)
                    || doc_offsets.last() != Some(&num_values)
                    || doc_offsets.windows(2).any(|pair| {
                        pair[0] > pair[1] || (cardinality == 2 && pair[1] - pair[0] > 1)
                    })
                {
                    return Err(invalid("invalid compact document offsets"));
                }
            }
        }
        position += index_len;
        let dictionary = if num_symbols == 0 {
            None
        } else {
            let mut offsets = Vec::with_capacity(num_symbols + 1);
            for entry in bytes[position..position + symbol_offsets_len]
                .as_chunks::<4>()
                .0
            {
                offsets.push(u32::from_le_bytes(*entry));
            }
            position += symbol_offsets_len;
            let dictionary_bytes = bytes[position..position + dict_bytes_len].to_vec();
            position += dict_bytes_len;
            Some(
                CompactDictionary::validate(OwnedDictionaryStorage::new(dictionary_bytes, offsets))
                    .map_err(|_| invalid("invalid OnPair dictionary"))?,
            )
        };
        let mut value_offsets = Vec::with_capacity(num_values as usize + 1);
        for entry in bytes[position..position + value_offsets_len].chunks_exact(width) {
            value_offsets.push(if width == 4 {
                read_u32(entry, 0)? as u64
            } else {
                read_u64(entry, 0)?
            });
        }
        position += value_offsets_len;
        if value_offsets.first() != Some(&0)
            || value_offsets.last() != Some(&(num_codes as u64))
            || value_offsets.windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(invalid("invalid compact value offsets"));
        }
        let mut codes = Vec::with_capacity(num_codes);
        for entry in bytes[position..].as_chunks::<2>().0 {
            let code = u16::from_le_bytes(*entry);
            if code as usize >= num_symbols {
                return Err(invalid("OnPair code out of range"));
            }
            codes.push(code);
        }
        // Validate decoded-size hints before any unsafe decoder use.
        let mut actual_bytes = 0u64;
        let mut actual_max = 0u64;
        if let Some(dictionary) = &dictionary {
            for bounds in value_offsets.windows(2) {
                let length = onpair::decoded_len(
                    &codes[bounds[0] as usize..bounds[1] as usize],
                    dictionary.as_view(),
                ) as u64;
                actual_bytes = actual_bytes
                    .checked_add(length)
                    .ok_or_else(|| invalid("compact decoded length overflow"))?;
                actual_max = actual_max.max(length);
            }
        }
        if actual_bytes != decoded_bytes || actual_max != max_value_len {
            return Err(invalid("compact decoded length mismatch"));
        }
        Ok(Self {
            doc_offsets,
            cardinality,
            dictionary,
            value_offsets,
            codes,
        })
    }

    fn decode(&self, value: u32, output: &mut Vec<u8>) -> io::Result<()> {
        let start = self.value_offsets[value as usize] as usize;
        let end = self.value_offsets[value as usize + 1] as usize;
        let dictionary = self
            .dictionary
            .as_ref()
            .ok_or_else(|| invalid("empty compact dictionary"))?;
        let codes = &self.codes[start..end];
        let length = onpair::decoded_len(codes, dictionary.as_view());
        output.clear();
        output
            .try_reserve(length)
            .map_err(|_| invalid("cannot allocate compact value buffer"))?;
        let written =
            onpair::try_decode_into(codes, dictionary.as_view(), output.spare_capacity_mut())
                .map_err(|_| invalid("invalid compact output size"))?;
        // The checked decoder initialized exactly `written` bytes in spare capacity.
        unsafe {
            output.set_len(written);
        }
        Ok(())
    }
}

/// Appends complete values without a whole-value hash table.
pub(crate) struct CompactColumnWriter {
    pub(crate) options: CompactCodecOptions,
    bytes: Vec<u8>,
    offsets: Vec<u64>,
    docs: Vec<DocId>,
}

impl CompactColumnWriter {
    pub(crate) fn new(options: CompactCodecOptions) -> Self {
        Self {
            options,
            bytes: Vec::new(),
            offsets: vec![0],
            docs: Vec::new(),
        }
    }

    pub(crate) fn record(&mut self, doc: DocId, value: &[u8]) {
        assert!(
            self.docs.last().is_none_or(|&previous| previous <= doc),
            "compact values must be recorded in document order"
        );
        self.bytes.extend_from_slice(value);
        self.offsets.push(self.bytes.len() as u64);
        self.docs.push(doc);
    }

    pub(crate) fn mem_usage(&self) -> usize {
        self.bytes.capacity() + self.offsets.capacity() * 8 + self.docs.capacity() * 4
    }

    pub(crate) fn first(&self, doc: DocId) -> Option<&[u8]> {
        let value = self.docs.partition_point(|&value_doc| value_doc < doc);
        if self.docs.get(value) != Some(&doc) {
            return None;
        }
        Some(&self.bytes[self.offsets[value] as usize..self.offsets[value + 1] as usize])
    }

    pub(crate) fn serialize(
        &self,
        num_docs: u32,
        old_to_new: Option<&[u32]>,
        output: &mut impl Write,
    ) -> io::Result<()> {
        self.options.validate()?;
        if self.docs.len() > u32::MAX as usize
            || self.docs.last().is_some_and(|&doc| doc >= num_docs)
        {
            return Err(input_error("compact column exceeds document/value limits"));
        }
        let mut values: Vec<usize> = (0..self.docs.len()).collect();
        if let Some(mapping) = old_to_new {
            if mapping.len() != num_docs as usize {
                return Err(input_error("invalid compact document mapping"));
            }
            let mut seen = vec![false; num_docs as usize];
            for &doc in mapping {
                if doc >= num_docs || std::mem::replace(&mut seen[doc as usize], true) {
                    return Err(input_error("compact mapping is not a permutation"));
                }
            }
            values.sort_by_key(|&value| mapping[self.docs[value] as usize]);
        }
        let doc_for = |value: usize| {
            old_to_new.map_or(self.docs[value], |mapping| {
                mapping[self.docs[value] as usize]
            })
        };
        let mut group_writer = CompactGroupWriter::new(self.options.clone(), output);
        let mut next_value = 0;
        for doc in 0..num_docs {
            let start = next_value;
            while next_value < values.len() && doc_for(values[next_value]) == doc {
                next_value += 1;
            }
            let selected = &values[start..next_value];
            let doc_bytes: u64 = selected
                .iter()
                .map(|&value| self.offsets[value + 1] - self.offsets[value])
                .sum();
            group_writer.start_document(doc_bytes)?;
            for &value in selected {
                group_writer.record_value(
                    &self.bytes[self.offsets[value] as usize..self.offsets[value + 1] as usize],
                )?;
            }
            group_writer.end_document()?;
        }
        group_writer.finish()
    }
}

/// Streaming serialization in final document order, also used by merges.
pub(crate) struct CompactGroupWriter<'a, W: Write> {
    options: CompactCodecOptions,
    output: &'a mut W,
    directory: Vec<GroupAddress>,
    bytes: Vec<u8>,
    value_offsets: Vec<u64>,
    doc_offsets: Vec<u32>,
    num_docs: u32,
    num_values: u32,
    written: u64,
    uncompressed: u64,
    cardinality: Cardinality,
}

impl<'a, W: Write> CompactGroupWriter<'a, W> {
    pub(crate) fn new(options: CompactCodecOptions, output: &'a mut W) -> Self {
        Self {
            options,
            output,
            directory: Vec::new(),
            bytes: Vec::new(),
            value_offsets: vec![0],
            doc_offsets: vec![0],
            num_docs: 0,
            num_values: 0,
            written: 1,
            uncompressed: 0,
            cardinality: Cardinality::Full,
        }
    }

    pub(crate) fn start_document(&mut self, bytes: u64) -> io::Result<()> {
        let group_docs = self.doc_offsets.len() - 1;
        if group_docs > 0
            && (group_docs >= self.options.row_group_max_docs as usize
                || (self.bytes.len() as u64).saturating_add(bytes)
                    > self.options.row_group_target_bytes)
        {
            self.flush_group()?;
        }
        Ok(())
    }

    pub(crate) fn record_value(&mut self, value: &[u8]) -> io::Result<()> {
        if self.num_values == u32::MAX || self.value_offsets.len() > u32::MAX as usize {
            return Err(input_error("too many compact values"));
        }
        self.bytes.extend_from_slice(value);
        self.value_offsets.push(self.bytes.len() as u64);
        self.num_values += 1;
        Ok(())
    }

    pub(crate) fn end_document(&mut self) -> io::Result<()> {
        let end = (self.value_offsets.len() - 1) as u32;
        let count = end - self.doc_offsets.last().copied().unwrap();
        self.cardinality = self.cardinality.max(match count {
            0 => Cardinality::Optional,
            1 => Cardinality::Full,
            _ => Cardinality::Multivalued,
        });
        self.doc_offsets.push(end);
        self.num_docs = self
            .num_docs
            .checked_add(1)
            .ok_or_else(|| input_error("too many compact documents"))?;
        Ok(())
    }

    fn flush_group(&mut self) -> io::Result<()> {
        let num_docs = (self.doc_offsets.len() - 1) as u32;
        if num_docs == 0 {
            return Ok(());
        }
        let num_values = (self.value_offsets.len() - 1) as u32;
        self.directory.push(GroupAddress {
            doc: self.num_docs - num_docs,
            value: self.num_values - num_values,
            offset: self.written,
        });
        let mut serialized = vec![0; HEADER_LEN];
        serialized[..2].copy_from_slice(&1u16.to_le_bytes());
        serialized[4..8].copy_from_slice(&num_docs.to_le_bytes());
        serialized[8..12].copy_from_slice(&num_values.to_le_bytes());
        serialized[24..32].copy_from_slice(&(self.bytes.len() as u64).to_le_bytes());
        let index_len = if num_values == 0 {
            0
        } else {
            let mut cardinality = Cardinality::Full;
            for bounds in self.doc_offsets.windows(2) {
                match bounds[1] - bounds[0] {
                    0 => cardinality = cardinality.max(Cardinality::Optional),
                    1 => {}
                    _ => cardinality = Cardinality::Multivalued,
                }
            }
            serialized[2] = cardinality.to_code() + 1;
            if cardinality != Cardinality::Full {
                for offset in &self.doc_offsets {
                    serialized.extend_from_slice(&offset.to_le_bytes());
                }
            }
            u32::try_from(serialized.len() - HEADER_LEN)
                .map_err(|_| input_error("compact document index exceeds u32"))?
        };
        serialized[36..40].copy_from_slice(&index_len.to_le_bytes());
        let (codes, row_offsets) = if num_values == 0 {
            (Vec::new(), vec![0u64])
        } else {
            let config = onpair::Config {
                max_dict_bits: onpair::MaxDictBits::new(self.options.max_dict_bits)
                    .map_err(|_| input_error("invalid OnPair dictionary budget"))?,
                ..onpair::DEFAULT_CONFIG
            };
            let column = onpair::compress(&self.bytes, &self.value_offsets, config)
                .map_err(|_| input_error("OnPair compression failed"))?;
            let (dictionary, codes, offsets) = column.into_raw();
            let (mut dictionary_bytes, dictionary_offsets) = dictionary.into_raw();
            let logical_length = dictionary_offsets.last().copied().unwrap() as usize;
            dictionary_bytes.resize(logical_length + 15, 0);
            serialized[12..16]
                .copy_from_slice(&((dictionary_offsets.len() - 1) as u32).to_le_bytes());
            serialized[32..36].copy_from_slice(&(dictionary_bytes.len() as u32).to_le_bytes());
            for offset in dictionary_offsets {
                serialized.extend_from_slice(&offset.to_le_bytes());
            }
            serialized.extend_from_slice(&dictionary_bytes);
            (codes, offsets)
        };
        let width = if codes.len() <= u32::MAX as usize {
            4
        } else {
            8
        };
        serialized[3] = width;
        serialized[16..24].copy_from_slice(&(codes.len() as u64).to_le_bytes());
        let max_value = self
            .value_offsets
            .windows(2)
            .map(|bounds| bounds[1] - bounds[0])
            .max()
            .unwrap_or(0);
        serialized[40..48].copy_from_slice(&max_value.to_le_bytes());
        for offset in row_offsets {
            if width == 4 {
                serialized.extend_from_slice(&(offset as u32).to_le_bytes());
            } else {
                serialized.extend_from_slice(&offset.to_le_bytes());
            }
        }
        for code in codes {
            serialized.extend_from_slice(&code.to_le_bytes());
        }
        let checksum = crc32fast::hash(&serialized);
        serialized[48..52].copy_from_slice(&checksum.to_le_bytes());
        self.output.write_all(&serialized)?;
        self.written += serialized.len() as u64;
        self.uncompressed += self.bytes.len() as u64;
        self.bytes.clear();
        self.value_offsets.clear();
        self.value_offsets.push(0);
        self.doc_offsets.clear();
        self.doc_offsets.push(0);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> io::Result<()> {
        self.flush_group()?;
        let num_groups = u32::try_from(self.directory.len())
            .map_err(|_| input_error("too many compact groups"))?;
        self.directory.push(GroupAddress {
            doc: self.num_docs,
            value: self.num_values,
            offset: self.written,
        });
        let mut directory_bytes = Vec::with_capacity(self.directory.len() * DIRECTORY_ENTRY_LEN);
        for address in self.directory {
            directory_bytes.extend_from_slice(&address.doc.to_le_bytes());
            directory_bytes.extend_from_slice(&address.value.to_le_bytes());
            directory_bytes.extend_from_slice(&address.offset.to_le_bytes());
        }
        self.output.write_all(&directory_bytes)?;
        let mut footer = [0; FOOTER_LEN];
        footer[..4].copy_from_slice(MAGIC);
        footer[4..6].copy_from_slice(&1u16.to_le_bytes());
        footer[8..12].copy_from_slice(&self.num_docs.to_le_bytes());
        footer[12..16].copy_from_slice(&num_groups.to_le_bytes());
        footer[16..24].copy_from_slice(&(self.num_values as u64).to_le_bytes());
        footer[24..32].copy_from_slice(&self.written.to_le_bytes());
        footer[32..40].copy_from_slice(&(directory_bytes.len() as u64).to_le_bytes());
        footer[40..48].copy_from_slice(&self.uncompressed.to_le_bytes());
        footer[48..52].copy_from_slice(&crc32fast::hash(&directory_bytes).to_le_bytes());
        footer[52] = self.cardinality.to_code();
        self.output.write_all(&footer)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn compact_random_binary_roundtrip(
            values in prop::collection::vec(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..5), 0..32),
            group_docs in 1u32..10,
            target_bytes in 1u64..512,
        ) {
            let borrowed: Vec<Vec<&[u8]>> = values.iter().map(|doc| doc.iter().map(Vec::as_slice).collect()).collect();
            let column = roundtrip(&borrowed, CompactCodecOptions { row_group_max_docs: group_docs, row_group_target_bytes: target_bytes, ..Default::default() });
            let mut buffer = Vec::new();
            for (doc, expected) in values.iter().enumerate().rev() {
                let range = column.value_range(doc as u32).unwrap();
                prop_assert_eq!(range.len(), expected.len());
                for (row, value) in range.zip(expected) {
                    column.read_value(row, &mut buffer).unwrap();
                    prop_assert_eq!(&buffer, value);
                    prop_assert_eq!(column.doc_for_value(row).unwrap(), doc as u32);
                }
            }
        }
    }

    #[derive(Debug)]
    struct RecordingFile {
        bytes: OwnedBytes,
        reads: Arc<Mutex<Vec<Range<usize>>>>,
    }

    impl common::HasLen for RecordingFile {
        fn len(&self) -> usize {
            self.bytes.len()
        }
    }
    impl common::file_slice::FileHandle for RecordingFile {
        fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
            self.reads.lock().unwrap().push(range.clone());
            Ok(self.bytes.slice(range))
        }
    }

    #[test]
    fn compact_reads_only_metadata_and_requested_group() {
        let mut writer = CompactColumnWriter::new(CompactCodecOptions {
            row_group_max_docs: 2,
            ..Default::default()
        });
        for doc in 0..8 {
            writer.record(doc, format!("value-{doc}").as_bytes());
        }
        let mut bytes = vec![1];
        writer.serialize(8, None, &mut bytes).unwrap();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let column = CompactColumn::open(FileSlice::new(Arc::new(RecordingFile {
            bytes: OwnedBytes::new(bytes),
            reads: reads.clone(),
        })))
        .unwrap();
        assert_eq!(reads.lock().unwrap().len(), 2); // footer and directory
        reads.lock().unwrap().clear();
        let mut value = Vec::new();
        column.first(4, &mut value).unwrap();
        assert_eq!(value, b"value-4");
        let group = column.group_range(4).unwrap();
        assert_eq!(
            *reads.lock().unwrap(),
            vec![group.start as usize..group.end as usize]
        );
        column.first(5, &mut value).unwrap();
        assert_eq!(reads.lock().unwrap().len(), 1); // same cached group
        column.first(1, &mut value).unwrap();
        assert_eq!(reads.lock().unwrap().len(), 2);
    }

    #[test]
    fn compact_remapping_preserves_value_order() {
        let mut writer = CompactColumnWriter::new(CompactCodecOptions {
            row_group_max_docs: 1,
            ..Default::default()
        });
        writer.record(0, b"z");
        writer.record(0, b"a");
        writer.record(2, b"b");
        let mut bytes = vec![1];
        writer.serialize(3, Some(&[2, 0, 1]), &mut bytes).unwrap();
        let column = CompactColumn::open(FileSlice::from(bytes)).unwrap();
        assert_eq!(column.value_range(0).unwrap(), 0..0);
        let mut buffer = Vec::new();
        for (row, expected) in [b"b", b"z", b"a"].into_iter().enumerate() {
            column.read_value(row as u32, &mut buffer).unwrap();
            assert_eq!(buffer, expected);
        }
        assert!(
            writer
                .serialize(3, Some(&[0, 0, 1]), &mut Vec::new())
                .is_err()
        );
    }

    #[test]
    fn compact_mixed_codec_merges_with_deletes_and_shuffle() {
        use crate::{
            ColumnType, ColumnarReader, ColumnarWriter, DynamicColumn, FastFieldCodec,
            MergeRowOrder, RowAddr, ShuffleMergeOrder, StackMergeOrder,
        };
        let compact = FastFieldCodec::Compact(CompactCodecOptions {
            row_group_max_docs: 1,
            ..Default::default()
        });
        let mut readers = Vec::new();
        for (segment, codec) in [FastFieldCodec::Dictionary, compact.clone()]
            .into_iter()
            .enumerate()
        {
            let mut writer = ColumnarWriter::default();
            writer
                .record_column_type_with_codec("value", ColumnType::Str, false, codec)
                .unwrap();
            writer.record_str(0, "value", if segment == 0 { "old" } else { "new" });
            writer.record_str(0, "value", "");
            writer.record_str(2, "value", "last");
            let mut serialized = Vec::new();
            writer.serialize(3, None, &mut serialized).unwrap();
            readers.push(ColumnarReader::open(serialized).unwrap());
        }
        let references: Vec<_> = readers.iter().collect();
        for codec in [FastFieldCodec::Dictionary, compact] {
            for shuffled in [false, true] {
                let order = if shuffled {
                    MergeRowOrder::Shuffled(ShuffleMergeOrder::for_test(
                        &[3, 3],
                        vec![
                            RowAddr {
                                segment_ord: 1,
                                row_id: 0,
                            },
                            RowAddr {
                                segment_ord: 0,
                                row_id: 1,
                            },
                            RowAddr {
                                segment_ord: 0,
                                row_id: 0,
                            },
                        ],
                    ))
                } else {
                    MergeRowOrder::Stack(StackMergeOrder::stack(&references))
                };
                let mut serialized = Vec::new();
                crate::merge_columnar_with_codec(
                    &references,
                    &[],
                    order,
                    |_, _| codec.clone(),
                    &mut serialized,
                )
                .unwrap();
                let reader = ColumnarReader::open(serialized).unwrap();
                let DynamicColumn::Str(column) =
                    reader.read_columns("value").unwrap()[0].open().unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    column.is_compact(),
                    matches!(codec, FastFieldCodec::Compact(_))
                );
                let expected = if shuffled {
                    vec![vec!["new", ""], vec![], vec!["old", ""]]
                } else {
                    vec![
                        vec!["old", ""],
                        vec![],
                        vec!["last"],
                        vec!["new", ""],
                        vec![],
                        vec!["last"],
                    ]
                };
                let mut buffer = Vec::new();
                for (doc, values) in expected.into_iter().enumerate() {
                    let rows = column.value_range(doc as u32).unwrap();
                    assert_eq!(rows.len(), values.len());
                    for (row, expected) in rows.zip(values) {
                        column.read_value(row, &mut buffer).unwrap();
                        assert_eq!(buffer, expected.as_bytes());
                    }
                }
            }
        }
    }

    #[test]
    fn compact_rejects_corrupt_data_without_panicking() {
        let mut writer = CompactColumnWriter::new(CompactCodecOptions::default());
        writer.record(0, b"a");
        writer.record(0, b"b");
        writer.record(2, b"c");
        let mut bytes = vec![1];
        writer.serialize(3, None, &mut bytes).unwrap();
        // Every single-byte corruption in groups or metadata is rejected either
        // at open or at the first read. Some unused footer statistics are exempt.
        let footer_start = bytes.len() - FOOTER_LEN;
        for position in 1..bytes.len() {
            if (footer_start + 40..footer_start + 48).contains(&position) {
                continue;
            }
            let mut corrupt = bytes.clone();
            corrupt[position] ^= 0x80;
            let result = CompactColumn::open(FileSlice::from(corrupt))
                .and_then(|column| column.first(0, &mut Vec::new()));
            assert!(result.is_err(), "corruption at {position} was accepted");
        }
        for length in 0..bytes.len() {
            assert!(CompactColumn::open(FileSlice::from(bytes[..length].to_vec())).is_err());
        }
        // Recompute the checksum after corrupting document offsets. This tests
        // structural validation independently of the checksum.
        bytes[1 + HEADER_LEN + 4..1 + HEADER_LEN + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        let group_end = read_u64(&bytes[footer_start..], 24).unwrap() as usize;
        bytes[49..53].fill(0);
        let checksum = crc32fast::hash(&bytes[1..group_end]);
        bytes[49..53].copy_from_slice(&checksum.to_le_bytes());
        let column = CompactColumn::open(FileSlice::from(bytes)).unwrap();
        assert!(column.first(0, &mut Vec::new()).is_err());
    }

    fn roundtrip(values: &[Vec<&[u8]>], options: CompactCodecOptions) -> CompactColumn {
        let mut writer = CompactColumnWriter::new(options);
        for (doc, values) in values.iter().enumerate() {
            for value in values {
                writer.record(doc as u32, value);
            }
        }
        let mut bytes = vec![1];
        writer
            .serialize(values.len() as u32, None, &mut bytes)
            .unwrap();
        CompactColumn::open(FileSlice::from(bytes)).unwrap()
    }

    #[test]
    fn compact_preserves_missing_empty_duplicates_and_groups() {
        let values: Vec<Vec<&[u8]>> = vec![
            vec![],
            vec![b""],
            vec![b"ax", b"by", b"ax"],
            vec![],
            vec![b"hello"],
        ];
        let column = roundtrip(
            &values,
            CompactCodecOptions {
                row_group_max_docs: 2,
                ..Default::default()
            },
        );
        assert_eq!(column.num_groups(), 3);
        let mut buffer = Vec::new();
        for (doc, expected) in values.iter().enumerate() {
            let range = column.value_range(doc as u32).unwrap();
            assert_eq!(range.len(), expected.len());
            for (value, expected) in range.zip(expected) {
                column.read_value(value, &mut buffer).unwrap();
                assert_eq!(&buffer, expected);
            }
        }
        assert_eq!(column.next_non_null_doc(0).unwrap(), Some(1));
        assert_eq!(column.doc_for_value(3).unwrap(), 2);
    }

    #[test]
    fn compact_empty_and_binary_columns() {
        let empty = roundtrip(&[], CompactCodecOptions::default());
        assert_eq!(empty.num_docs(), 0);
        assert_eq!(empty.num_groups(), 0);
        let binary: Vec<u8> = (0..=255).collect();
        let values = vec![vec![binary.as_slice()], vec![], vec![b"".as_slice()]];
        let column = roundtrip(
            &values,
            CompactCodecOptions {
                row_group_target_bytes: 3,
                ..Default::default()
            },
        );
        let mut buffer = Vec::new();
        assert!(column.first(0, &mut buffer).unwrap());
        assert_eq!(buffer, binary);
        assert!(!column.first(1, &mut buffer).unwrap());
        assert!(column.first(2, &mut buffer).unwrap());
        assert!(buffer.is_empty());
    }
}
