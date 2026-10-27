use std::io;

use common::OwnedBytes;
use tantivy_sstable::value::{ValueWriter, VecU32ValueReader, VecU32ValueWriter};
use tantivy_sstable::{BlockReader, Dictionary, SSTable, VoidSSTable, encode_block_parts};

fn frame_block_body(body: &[u8]) -> Vec<u8> {
    let mut bytes = (body.len() as u32).to_le_bytes().to_vec();
    bytes.extend_from_slice(body);
    bytes
}

fn read_body(body: &[u8]) -> io::Result<Vec<u8>> {
    let mut reader = BlockReader::new(OwnedBytes::new(frame_block_body(body)));
    assert!(reader.read_block()?);
    Ok(reader.buffer().to_vec())
}

#[test]
fn roundtrip_parts() {
    for (metadata, payload, keys) in [
        (vec![], vec![], vec![]),
        (b"meta".to_vec(), b"payload".to_vec(), b"keys".to_vec()),
        (vec![7; 4000], vec![8; 4000], vec![9; 4000]),
    ] {
        let mut body = Vec::new();
        encode_block_parts(&metadata, &payload, &keys, &mut body).unwrap();
        assert_eq!(body[0], 2);
        let mut pos = 1;
        for (index, original) in [&metadata, &payload, &keys].into_iter().enumerate() {
            let decoded = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
            let stored = u32::from_le_bytes(body[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let codec = body[pos + 8];
            assert_eq!(decoded, original.len());
            if index == 1 || !cfg!(feature = "zstd-compression") || original.len() < 100 {
                assert_eq!(codec, 0);
                assert_eq!(&body[pos + 9..pos + 9 + stored], original);
            } else {
                assert_eq!(codec, 1);
                assert!(stored < decoded);
            }
            pos += 9 + stored;
        }
        assert_eq!(pos, body.len());
        assert_eq!(
            read_body(&body).unwrap(),
            [metadata, payload, keys].concat()
        );
    }
}

#[test]
fn rejects_corrupt_parts() {
    let mut body = Vec::new();
    encode_block_parts(b"meta", b"payload", b"keys", &mut body).unwrap();
    // Every truncation, including truncation of the outer length or flag.
    let bytes = frame_block_body(&body);
    for len in 1..bytes.len() {
        let mut reader = BlockReader::new(OwnedBytes::new(bytes[..len].to_vec()));
        assert!(reader.read_block().is_err(), "outer truncation {len}");
    }
    for len in 1..body.len() {
        assert!(read_body(&body[..len]).is_err(), "part truncation {len}");
    }
    for (offset, replacement) in [(1, u32::MAX), (5, u32::MAX), (1, 3)] {
        let mut corrupt = body.clone();
        corrupt[offset..offset + 4].copy_from_slice(&replacement.to_le_bytes());
        assert!(read_body(&corrupt).is_err());
    }
    for offset in [9, 22, 38] {
        let mut corrupt = body.clone();
        corrupt[offset] = 99;
        assert!(read_body(&corrupt).is_err());
    }
    body.push(0);
    assert!(read_body(&body).is_err());
    assert!(read_body(&[99, 0]).is_err());
}

#[test]
fn rejects_invalid_compressed_parts() {
    let mut body = Vec::new();
    encode_block_parts(b"meta", b"payload", b"keys", &mut body).unwrap();
    body[9] = 1;
    #[cfg(feature = "zstd-compression")]
    assert!(read_body(&body).is_err());
    #[cfg(not(feature = "zstd-compression"))]
    {
        assert_eq!(
            read_body(&body).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            read_body(&[1, 0]).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
    body[9] = 0;
    body[22] = 1; // The payload must remain raw.
    assert_eq!(
        read_body(&body).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[cfg(feature = "zstd-compression")]
#[test]
fn rejects_decoded_length_mismatch() {
    let mut body = Vec::new();
    encode_block_parts(&vec![0; 4000], b"", b"", &mut body).unwrap();
    for length in [0u32, 3999, 4001, u32::MAX] {
        body[1..5].copy_from_slice(&length.to_le_bytes());
        assert!(read_body(&body).is_err());
    }
}

#[test]
fn whole_block_compression_roundtrip() {
    // With zstd enabled, this block is compressed as a whole.
    let key = vec![b'a'; 3000];
    let mut writer = VoidSSTable::writer(Vec::new());
    writer.insert(&key, &()).unwrap();
    let bytes = writer.finish().unwrap();
    let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let mut raw = vec![1, 0, 0xb8, 0x17]; // keep=0, add=3000 as vints
    raw.extend_from_slice(&key);
    let mut expected = vec![0];
    expected.extend_from_slice(&raw);
    #[cfg(feature = "zstd-compression")]
    {
        expected = vec![1];
        expected.extend_from_slice(&zstd::bulk::compress(&raw, 3).unwrap());
    }
    assert_eq!(&bytes[4..4 + len], &expected);
    assert_eq!(read_body(&expected).unwrap(), raw);
}

proptest::proptest! {
    #[test]
    fn malformed_parts_do_not_panic(data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256)) {
        let mut body = vec![2];
        body.extend_from_slice(&data);
        let _ = read_body(&body);
    }
}

#[derive(Default)]
struct PartsWriter(VecU32ValueWriter);

impl ValueWriter for PartsWriter {
    type Value = Vec<u32>;

    fn write(&mut self, value: &Self::Value) {
        self.0.write(value);
    }

    fn serialize_block(&self, _: &mut Vec<u8>) {
        panic!("serialize_block_with_keys supplies the block body");
    }

    fn serialize_block_with_keys(&self, keys: &[u8], buffer: &mut Vec<u8>) -> io::Result<bool> {
        assert!(buffer.is_empty());
        let mut values = Vec::new();
        self.0.serialize_block(&mut values);
        encode_block_parts(&values[..4], &values[4..], keys, buffer)?;
        Ok(true)
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

struct PartsTable;

impl SSTable for PartsTable {
    type Value = Vec<u32>;
    type ValueWriter = PartsWriter;
    type ValueReader = VecU32ValueReader;
}

#[test]
fn dictionary_and_streamer_read_parts_across_blocks() {
    let mut writer = Dictionary::<PartsTable>::builder(Vec::new()).unwrap();
    writer.set_block_len(1);
    for (i, key) in [b"apple", b"berry", b"cherry".as_slice()]
        .into_iter()
        .enumerate()
    {
        writer.insert(key, &vec![i as u32; 1000]).unwrap();
    }
    let bytes = writer.finish().unwrap();
    assert_eq!(bytes[4], 2);
    assert_eq!(&bytes[bytes.len() - 4..], &4u32.to_le_bytes());
    let dictionary = Dictionary::<PartsTable>::from_bytes(OwnedBytes::new(bytes)).unwrap();
    assert_eq!(dictionary.get(b"berry").unwrap(), Some(vec![1; 1000]));
    let mut stream = dictionary.stream().unwrap();
    for (i, key) in [b"apple", b"berry", b"cherry".as_slice()]
        .into_iter()
        .enumerate()
    {
        assert!(stream.advance());
        assert_eq!(stream.key(), key);
        assert_eq!(stream.value(), &vec![i as u32; 1000]);
        assert_eq!(stream.term_ord(), i as u64);
    }
    assert!(!stream.advance());
}
