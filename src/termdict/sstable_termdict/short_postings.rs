use std::io::{self, Read};
use std::sync::Arc;

use common::VInt;
use tantivy_bitpacker::{BitPacker, BitUnpacker};

use crate::postings::ShortPostings;

const POSITION_TAG: u32 = 1 << 31;
const VERSION: u64 = 2;

pub(super) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn read_usize(data: &mut &[u8]) -> io::Result<usize> {
    usize::try_from(VInt::deserialize_u64(data)?)
        .map_err(|_| invalid("term metadata integer overflow"))
}

fn count_and_positions_bits(max_inline_docs: u32) -> usize {
    (u32::BITS - max_inline_docs.saturating_sub(1).leading_zeros() + 1) as usize
}

fn write_lanes(values: &[u32], output: &mut Vec<u8>) -> io::Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    for shift in [0, 8, 16, 24] {
        let lane: Vec<u8> = values.iter().map(|value| (value >> shift) as u8).collect();
        let compressed = zstd::bulk::compress(&lane, 3)?;
        VInt(compressed.len() as u64).serialize_into_vec(output);
        output.extend_from_slice(&compressed);
    }
    Ok(())
}

fn read_lanes(data: &mut &[u8], count: usize) -> io::Result<Vec<u32>> {
    let mut values = Vec::new();
    if count == 0 {
        return Ok(values);
    }
    for shift in [0, 8, 16, 24] {
        let len = read_usize(data)?;
        let encoded = data
            .get(..len)
            .ok_or_else(|| invalid("truncated byte lane"))?;
        *data = &data[len..];
        // Decompress before allocating the integer array from an untrusted count.
        let mut lane = Vec::new();
        zstd::stream::read::Decoder::new(encoded)?
            .take(count as u64 + 1)
            .read_to_end(&mut lane)?;
        if lane.len() != count {
            return Err(invalid("byte lane length mismatch"));
        }
        if shift == 0 {
            values.try_reserve_exact(count).map_err(io::Error::other)?;
            values.extend(lane.into_iter().map(u32::from));
        } else {
            for (value, byte) in values.iter_mut().zip(lane) {
                *value |= u32::from(byte) << shift;
            }
        }
    }
    Ok(values)
}

pub(super) fn write_block(
    block: &super::TermInfoBlock,
    keys: &[u8],
    output: &mut Vec<u8>,
) -> io::Result<()> {
    let terms = &block.term_infos;
    let max_inline_docs = block.max_inline_docs;
    let count_and_positions_bits = count_and_positions_bits(max_inline_docs);
    let positions_flag = 1u8 << (count_and_positions_bits - 1);
    let mut metadata = Vec::new();
    // Zero cannot be a nonempty legacy block's term count.
    for value in [
        0,
        VERSION,
        terms.len() as u64,
        terms[0].postings_range.start as u64,
        terms[0].positions_range.start as u64,
    ] {
        VInt(value).serialize_into_vec(&mut metadata);
    }
    let mut counts = Vec::new();
    let mut packer = BitPacker::new();
    let mut docs = Vec::new();
    let mut freqs = Vec::new();
    let mut positions = Vec::new();
    let mut postings_end = terms[0].postings_range.start;
    let mut positions_end = terms[0].positions_range.start;
    for term in terms {
        if term.postings_range.start != postings_end
            || term.positions_range.start != positions_end
            || term.postings_range.end < postings_end
            || term.positions_range.end < positions_end
        {
            return Err(invalid("non-contiguous term ranges"));
        }
        postings_end = term.postings_range.end;
        positions_end = term.positions_range.end;
        if let Some(short) = &term.short_postings {
            short.validate()?;
            if term.doc_freq as usize != short.doc_ids.len()
                || term.doc_freq > max_inline_docs
                || !term.postings_range.is_empty()
                || !term.positions_range.is_empty()
            {
                return Err(invalid("short postings metadata mismatch"));
            }
            VInt(0).serialize_into_vec(&mut metadata);
            let has_positions = !short.position_deltas.is_empty();
            let flag = (term.doc_freq as u8 - 1) | (u8::from(has_positions) * positions_flag);
            packer.write(u64::from(flag), count_and_positions_bits as u8, &mut counts)?;
            // Reset per list: the first ID is absolute, subsequent values are gaps.
            let mut previous = 0;
            for &doc in &short.doc_ids {
                docs.push(doc - previous);
                previous = doc;
            }
            if short.term_freqs.is_empty() {
                // Zero distinguishes frequency-free JSON values from text frequencies of one.
                freqs.resize(freqs.len() + short.doc_ids.len(), 0);
            }
            let mut deltas = short.position_deltas.as_slice();
            for &freq in &short.term_freqs {
                if has_positions && freq == 1 {
                    freqs.push(POSITION_TAG | deltas[0]);
                } else {
                    freqs.push(freq);
                    if has_positions {
                        positions.extend_from_slice(&deltas[..freq as usize]);
                    }
                }
                if has_positions {
                    deltas = &deltas[freq as usize..];
                }
            }
        } else {
            if term.doc_freq == 0 {
                return Err(invalid("empty external postings list"));
            }
            for value in [
                term.doc_freq as u64,
                term.postings_range.len() as u64,
                term.positions_range.len() as u64,
            ] {
                VInt(value).serialize_into_vec(&mut metadata);
            }
        }
    }
    packer.flush(&mut counts)?;
    metadata.extend_from_slice(&counts);
    let mut payload = Vec::new();
    for stream in [&docs, &freqs, &positions] {
        write_lanes(stream, &mut payload)?;
    }
    sstable::encode_block_parts(&metadata, &payload, keys, output)
}

pub(super) fn read_block(mut data: &[u8], block: &mut super::TermInfoBlock) -> io::Result<usize> {
    let max_inline_docs = block.max_inline_docs;
    let count_and_positions_bits = count_and_positions_bits(max_inline_docs);
    let positions_flag = 1u8 << (count_and_positions_bits - 1);
    let start_len = data.len();
    let version = VInt::deserialize_u64(&mut data)?;
    if version != 1 && version != VERSION {
        return Err(invalid("unsupported short postings version"));
    }
    let count = read_usize(&mut data)?;
    let short_count = block.read_term_infos(&mut data, count, true)?;
    if short_count == 0 {
        return Err(invalid("missing short postings"));
    }
    let count_bytes = short_count
        .checked_mul(count_and_positions_bits)
        .ok_or_else(|| invalid("short counts overflow"))?
        .div_ceil(8);
    let counts = data
        .get(..count_bytes)
        .ok_or_else(|| invalid("truncated short counts"))?;
    data = &data[count_bytes..];
    let padding_start = (short_count * count_and_positions_bits) % 8;
    if padding_start != 0 && counts[count_bytes - 1] >> padding_start != 0 {
        return Err(invalid("invalid count padding"));
    }
    let unpacker = BitUnpacker::new(count_and_positions_bits as u8);
    let short_count = u32::try_from(short_count).map_err(|_| invalid("short counts overflow"))?;
    let flags = (0..short_count).map(|i| unpacker.get(i, counts) as u8);
    let doc_count = flags.clone().try_fold(0usize, |total, flag| {
        let count = (flag & (positions_flag - 1)) as u32 + 1;
        if count > max_inline_docs {
            return Err(invalid("short posting count exceeds dictionary maximum"));
        }
        total
            .checked_add(count as usize)
            .ok_or_else(|| invalid("document count overflow"))
    })?;
    let docs = read_lanes(&mut data, doc_count)?;
    let freqs = read_lanes(&mut data, doc_count)?;
    let mut doc_offset = 0;
    let mut position_count = 0usize;
    for flag in flags.clone() {
        let count = (flag & (positions_flag - 1)) as usize + 1;
        for &freq in &freqs[doc_offset..doc_offset + count] {
            if freq & POSITION_TAG != 0 {
                if flag & positions_flag == 0 {
                    return Err(invalid("embedded position without positions"));
                }
            } else if flag & positions_flag != 0 {
                if freq == 0 {
                    return Err(invalid("positions without frequencies"));
                }
                position_count = position_count
                    .checked_add(freq as usize)
                    .ok_or_else(|| invalid("position count overflow"))?;
            }
        }
        doc_offset += count;
    }
    let positions = read_lanes(&mut data, position_count)?;
    doc_offset = 0;
    let mut position_offset = 0;
    for (term, flag) in block
        .term_infos
        .iter_mut()
        .filter(|term| term.doc_freq == 0)
        .zip(flags)
    {
        let count = (flag & (positions_flag - 1)) as usize + 1;
        let end = doc_offset + count;
        let mut short = ShortPostings {
            doc_ids: docs[doc_offset..end].to_vec(),
            term_freqs: Vec::with_capacity(count),
            position_deltas: Vec::new(),
        };
        if version == VERSION {
            let mut previous = 0u32;
            for doc in &mut short.doc_ids {
                *doc = previous
                    .checked_add(*doc)
                    .ok_or_else(|| invalid("document ID delta overflow"))?;
                previous = *doc;
            }
        }
        for &freq in &freqs[doc_offset..end] {
            if freq & POSITION_TAG != 0 {
                short.term_freqs.push(1);
                short.position_deltas.push(freq & !POSITION_TAG);
            } else if freq != 0 {
                short.term_freqs.push(freq);
                if flag & positions_flag != 0 {
                    let end = position_offset + freq as usize;
                    short
                        .position_deltas
                        .extend_from_slice(&positions[position_offset..end]);
                    position_offset = end;
                }
            }
        }
        short.validate()?;
        term.doc_freq = count as u32;
        term.short_postings = Some(Arc::new(short));
        doc_offset = end;
    }
    Ok(start_len - data.len())
}

#[cfg(test)]
mod tests {
    use sstable::value::ValueReader;

    use super::super::TermInfoBlock;
    use super::*;
    use crate::postings::TermInfo;

    #[test]
    fn byte_lanes_roundtrip_and_truncation() -> io::Result<()> {
        let values = [0, 1, 255, 256, 65536, POSITION_TAG, u32::MAX];
        let mut encoded = Vec::new();
        write_lanes(&values, &mut encoded)?;
        assert_eq!(read_lanes(&mut encoded.as_slice(), values.len())?, values);
        for len in 0..encoded.len() {
            assert!(read_lanes(&mut &encoded[..len], values.len()).is_err());
        }
        assert!(read_lanes(&mut encoded.as_slice(), values.len() - 1).is_err());
        Ok(())
    }

    #[test]
    fn tagged_position_and_corrupt_blocks() -> io::Result<()> {
        for maximum in 1..=32 {
            let count_and_positions_bits = count_and_positions_bits(maximum);
            let mut bytes = Vec::new();
            for value in [VERSION, 1, 0, 0, 0] {
                VInt(value).serialize_into_vec(&mut bytes);
            }
            bytes.push(1 << (count_and_positions_bits - 1)); // One document with positions.
            write_lanes(&[9], &mut bytes)?;
            write_lanes(&[POSITION_TAG | 17], &mut bytes)?;
            let mut block = TermInfoBlock::new(maximum)?;
            assert_eq!(read_block(&bytes, &mut block)?, bytes.len());
            let short = block.term_infos[0].short_postings.as_ref().unwrap();
            assert_eq!(short.term_freqs, [1]);
            assert_eq!(short.position_deltas, [17]);
            for len in 0..bytes.len() {
                assert!(read_block(&bytes[..len], &mut TermInfoBlock::new(maximum)?).is_err());
            }
            bytes[5] = 0; // The tag requires positions.
            assert!(read_block(&bytes, &mut TermInfoBlock::new(maximum)?).is_err());
            for bit in count_and_positions_bits..8 {
                bytes[5] = 1 << bit;
                let error = read_block(&bytes, &mut TermInfoBlock::new(maximum)?).unwrap_err();
                assert_eq!(error.to_string(), "invalid count padding");
            }
            if !maximum.is_power_of_two() {
                bytes[5] = maximum as u8; // Encodes maximum + 1 without positions.
                let error = read_block(&bytes, &mut TermInfoBlock::new(maximum)?).unwrap_err();
                assert_eq!(
                    error.to_string(),
                    "short posting count exceeds dictionary maximum"
                );
            }
        }
        assert!(read_block(&[0; 11], &mut TermInfoBlock::new(32)?).is_err());
        for docs in [
            vec![crate::TERMINATED],
            vec![crate::TERMINATED, crate::TERMINATED + 1],
        ] {
            let mut bytes = Vec::new();
            for value in [VERSION, 1, 0, 0, 0] {
                VInt(value).serialize_into_vec(&mut bytes);
            }
            bytes.push(docs.len() as u8 - 1);
            write_lanes(&docs, &mut bytes)?;
            write_lanes(&vec![1; docs.len()], &mut bytes)?;
            assert!(read_block(&bytes, &mut TermInfoBlock::new(32)?).is_err());
        }
        assert_eq!(count_and_positions_bits(1), 1);
        assert_eq!(count_and_positions_bits(8), 4);
        assert_eq!(count_and_positions_bits(9), 5);
        assert_eq!(count_and_positions_bits(32), 6);
        Ok(())
    }

    #[test]
    fn block_streams_and_positions_roundtrip() -> io::Result<()> {
        use futures::executor::block_on;

        assert!(TermInfoBlock::new(33).is_err());
        for maximum in 0..=32 {
            let mut expected = Vec::new();
            for count in 1..=maximum {
                for with_positions in [false, true] {
                    let freqs: Vec<u32> =
                        (0..count).map(|i| if i % 2 == 0 { 1 } else { 3 }).collect();
                    let deltas = if with_positions {
                        freqs
                            .iter()
                            .flat_map(|&f| (0..f).map(|i| i * 65536))
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let term = TermInfo {
                        doc_freq: count,
                        postings_range: 5..5,
                        positions_range: 7..7,
                        short_postings: Some(Arc::new(ShortPostings {
                            doc_ids: (0..count).map(|i| i * 1_000_000).collect(),
                            term_freqs: freqs,
                            position_deltas: deltas,
                        })),
                    };
                    expected.push(term);
                }
            }
            let external = TermInfo {
                doc_freq: 129,
                postings_range: 5..90,
                positions_range: 7..400,
                short_postings: None,
            };
            expected.push(external);
            // Exercise the public writer/reader path, including independent block frames.
            let mut table =
                sstable::Writer::with_value_writer(Vec::new(), TermInfoBlock::new(maximum)?);
            table.set_block_len(1);
            for (i, term) in expected.iter().enumerate() {
                table.insert([b'a' + i as u8], term)?;
            }
            let dictionary = sstable::Dictionary::<super::super::TermSSTable>::from_bytes(
                common::OwnedBytes::new(table.finish()?),
            )?
            .with_value_config(maximum as u8)?
            .clone();
            for (i, term) in expected.iter().enumerate() {
                let key = [b'a' + i as u8];
                assert_eq!(dictionary.get(key)?.as_ref(), Some(term));
                assert_eq!(block_on(dictionary.get_async(key))?.as_ref(), Some(term));
                assert_eq!(
                    dictionary.term_info_from_ord(i as u64)?.as_ref(),
                    Some(term)
                );
            }
            for mut stream in [
                dictionary.stream()?,
                block_on(dictionary.range().into_stream_async())?,
            ] {
                for term in &expected {
                    assert!(stream.advance());
                    assert_eq!(stream.value(), term);
                }
                assert!(!stream.advance());
            }
            let automaton = tantivy_fst::Regex::new("a|e").unwrap();
            for gap in [None, Some(0), Some(512 * 1024)] {
                let builder = dictionary.search(&automaton).without_keys();
                let mut stream = match gap {
                    None => builder.into_stream()?,
                    Some(gap) => block_on(builder.into_stream_async_merging_holes(gap))?,
                };
                for i in [0, 4].into_iter().filter(|&i| i < expected.len()) {
                    assert!(stream.advance());
                    assert_eq!(stream.term_ord(), i as u64);
                    assert_eq!(stream.value(), &expected[i]);
                }
                assert!(!stream.advance());
            }
        }
        Ok(())
    }

    // Construct payloads independently of the writer to distinguish the two versions.
    fn document_lists(version: u64, lists: &[&[u32]]) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        for value in [version, lists.len() as u64, 0, 0] {
            VInt(value).serialize_into_vec(&mut bytes);
        }
        for _ in lists {
            VInt(0).serialize_into_vec(&mut bytes);
        }
        let mut packer = BitPacker::new();
        for list in lists {
            packer.write((list.len() - 1) as u64, 4, &mut bytes)?;
        }
        packer.flush(&mut bytes)?;
        let docs: Vec<_> = lists.iter().flat_map(|list| list.iter().copied()).collect();
        write_lanes(&docs, &mut bytes)?;
        write_lanes(&vec![1; docs.len()], &mut bytes)?;
        Ok(bytes)
    }

    #[test]
    fn document_versions_and_list_reset() -> io::Result<()> {
        for (version, lists) in [
            (1, vec![vec![100, 105, 115], vec![0, 3], vec![7]]),
            (2, vec![vec![100, 5, 10], vec![0, 3], vec![7]]),
        ] {
            let slices: Vec<_> = lists.iter().map(Vec::as_slice).collect();
            let bytes = document_lists(version, &slices)?;
            let mut block = TermInfoBlock::new(8)?;
            assert_eq!(read_block(&bytes, &mut block)?, bytes.len());
            let docs: Vec<_> = block
                .term_infos
                .iter()
                .map(|term| term.short_postings.as_ref().unwrap().doc_ids.as_slice())
                .collect();
            assert_eq!(docs, [&[100, 105, 115][..], &[0, 3], &[7]]);
        }
        Ok(())
    }

    #[test]
    fn document_deltas_reject_overflow_and_duplicate_ids() -> io::Result<()> {
        for (version, docs) in [
            (2, vec![1, u32::MAX]),
            (2, vec![4, 0]),
            (1, vec![4, 4]),
            (1, vec![4, 3]),
            (3, vec![1]),
        ] {
            let bytes = document_lists(version, &[&docs])?;
            assert!(read_block(&bytes, &mut TermInfoBlock::new(8)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn legacy_metadata_decodes_external_ranges() -> io::Result<()> {
        for doc_freq in [0, 129] {
            let mut bytes = Vec::new();
            for n in [1, 5, 7, doc_freq, 85, 393] {
                VInt(n).serialize_into_vec(&mut bytes);
            }
            let mut reader = TermInfoBlock::default();
            assert_eq!(reader.load(&bytes)?, bytes.len());
            assert_eq!(
                reader.value(0),
                &TermInfo {
                    doc_freq: doc_freq as u32,
                    postings_range: 5..90,
                    positions_range: 7..400,
                    short_postings: None,
                }
            );
        }
        Ok(())
    }
}
