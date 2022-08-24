use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;

use common::BinarySerializable;
use fastfield_codecs::bitpacked::BitpackedFastFieldReader as BitpackedReader;
use fastfield_codecs::blockwise_linear::BlockwiseLinearFastFieldReader;
use fastfield_codecs::linear::LinearFastFieldReader;
use fastfield_codecs::{FastFieldCodecReader, FastFieldCodecType};

use super::{FastValue, GCDFastFieldCodec};
use crate::directory::{CompositeFile, Directory, FileSlice, OwnedBytes, RamDirectory, WritePtr};
use crate::error::DataCorruption;
use crate::fastfield::{CompositeFastFieldSerializer, FastFieldsWriter};
use crate::schema::{Schema, FAST};
use crate::DocId;

/// FastFieldReader is the trait to access fast field data.
pub trait FastFieldReader<Item: FastValue>: Clone {
    /// Return the value associated to the given document.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    fn get(&self, doc: DocId) -> Item;

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// Regardless of the type of `Item`, this method works
    /// - transmuting the output array
    /// - extracting the `Item`s as if they were `u64`
    /// - possibly converting the `u64` value to the right type.
    ///
    /// # Panics
    ///
    /// May panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    fn get_range(&self, start: u64, output: &mut [Item]);

    /// Returns the minimum value for this fast field.
    ///
    /// The min value does not take in account of possible
    /// deleted document, and should be considered as a lower bound
    /// of the actual minimum value.
    fn min_value(&self) -> Item;

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    fn max_value(&self) -> Item;
}

#[derive(Clone)]
/// DynamicFastFieldReader wraps different readers to access
/// the various encoded fastfield data
pub enum DynamicFastFieldReader<Item: FastValue> {
    /// Bitpacked compressed fastfield data.
    Bitpacked(FastFieldReaderCodecWrapper<Item, BitpackedReader>),
    /// Linear interpolated values + bitpacked
    Linear(FastFieldReaderCodecWrapper<Item, LinearFastFieldReader>),
    /// Blockwise linear interpolated values + bitpacked
    BlockwiseLinear(FastFieldReaderCodecWrapper<Item, BlockwiseLinearFastFieldReader>),

    /// GCD and Bitpacked compressed fastfield data.
    BitpackedGCD(FastFieldReaderCodecWrapper<Item, GCDFastFieldCodec<BitpackedReader>>),
    /// GCD and Linear interpolated values + bitpacked
    LinearGCD(FastFieldReaderCodecWrapper<Item, GCDFastFieldCodec<LinearFastFieldReader>>),
    /// GCD and Blockwise linear interpolated values + bitpacked
    BlockwiseLinearGCD(
        FastFieldReaderCodecWrapper<Item, GCDFastFieldCodec<BlockwiseLinearFastFieldReader>>,
    ),
}

impl<Item: FastValue> DynamicFastFieldReader<Item> {
    /// Returns correct the reader wrapped in the `DynamicFastFieldReader` enum for the data.
    pub fn open_from_id(
        mut bytes: OwnedBytes,
        codec_type: FastFieldCodecType,
    ) -> crate::Result<DynamicFastFieldReader<Item>> {
        let reader = match codec_type {
            FastFieldCodecType::Bitpacked => {
                DynamicFastFieldReader::Bitpacked(FastFieldReaderCodecWrapper::<
                    Item,
                    BitpackedReader,
                >::open_from_bytes(bytes)?)
            }
            FastFieldCodecType::Linear => {
                DynamicFastFieldReader::Linear(FastFieldReaderCodecWrapper::<
                    Item,
                    LinearFastFieldReader,
                >::open_from_bytes(bytes)?)
            }
            FastFieldCodecType::BlockwiseLinear => {
                DynamicFastFieldReader::BlockwiseLinear(FastFieldReaderCodecWrapper::<
                    Item,
                    BlockwiseLinearFastFieldReader,
                >::open_from_bytes(bytes)?)
            }
            FastFieldCodecType::Gcd => {
                let codec_type = FastFieldCodecType::deserialize(&mut bytes)?;
                match codec_type {
                    FastFieldCodecType::Bitpacked => {
                        DynamicFastFieldReader::BitpackedGCD(FastFieldReaderCodecWrapper::<
                            Item,
                            GCDFastFieldCodec<BitpackedReader>,
                        >::open_from_bytes(
                            bytes
                        )?)
                    }
                    FastFieldCodecType::Linear => {
                        DynamicFastFieldReader::LinearGCD(FastFieldReaderCodecWrapper::<
                            Item,
                            GCDFastFieldCodec<LinearFastFieldReader>,
                        >::open_from_bytes(
                            bytes
                        )?)
                    }
                    FastFieldCodecType::BlockwiseLinear => {
                        DynamicFastFieldReader::BlockwiseLinearGCD(FastFieldReaderCodecWrapper::<
                            Item,
                            GCDFastFieldCodec<BlockwiseLinearFastFieldReader>,
                        >::open_from_bytes(
                            bytes
                        )?)
                    }
                    FastFieldCodecType::Gcd => {
                        return Err(DataCorruption::comment_only(
                            "Gcd codec wrapped into another gcd codec. This combination is not \
                             allowed.",
                        )
                        .into())
                    }
                }
            }
        };
        Ok(reader)
    }

    /// Returns correct the reader wrapped in the `DynamicFastFieldReader` enum for the data.
    pub fn open(file: FileSlice) -> crate::Result<DynamicFastFieldReader<Item>> {
        let mut bytes = file.read_bytes()?;
        let codec_type = FastFieldCodecType::deserialize(&mut bytes)?;
        Self::open_from_id(bytes, codec_type)
    }
}

impl<Item: FastValue> FastFieldReader<Item> for DynamicFastFieldReader<Item> {
    #[inline]
    fn get(&self, doc: DocId) -> Item {
        match self {
            Self::Bitpacked(reader) => reader.get(doc),
            Self::Linear(reader) => reader.get(doc),
            Self::BlockwiseLinear(reader) => reader.get(doc),
            Self::BitpackedGCD(reader) => reader.get(doc),
            Self::LinearGCD(reader) => reader.get(doc),
            Self::BlockwiseLinearGCD(reader) => reader.get(doc),
        }
    }
    #[inline]
    fn get_range(&self, start: u64, output: &mut [Item]) {
        match self {
            Self::Bitpacked(reader) => reader.get_range(start, output),
            Self::Linear(reader) => reader.get_range(start, output),
            Self::BlockwiseLinear(reader) => reader.get_range(start, output),
            Self::BitpackedGCD(reader) => reader.get_range(start, output),
            Self::LinearGCD(reader) => reader.get_range(start, output),
            Self::BlockwiseLinearGCD(reader) => reader.get_range(start, output),
        }
    }
    fn min_value(&self) -> Item {
        match self {
            Self::Bitpacked(reader) => reader.min_value(),
            Self::Linear(reader) => reader.min_value(),
            Self::BlockwiseLinear(reader) => reader.min_value(),
            Self::BitpackedGCD(reader) => reader.min_value(),
            Self::LinearGCD(reader) => reader.min_value(),
            Self::BlockwiseLinearGCD(reader) => reader.min_value(),
        }
    }
    fn max_value(&self) -> Item {
        match self {
            Self::Bitpacked(reader) => reader.max_value(),
            Self::Linear(reader) => reader.max_value(),
            Self::BlockwiseLinear(reader) => reader.max_value(),
            Self::BitpackedGCD(reader) => reader.max_value(),
            Self::LinearGCD(reader) => reader.max_value(),
            Self::BlockwiseLinearGCD(reader) => reader.max_value(),
        }
    }
}

/// Wrapper for accessing a fastfield.
///
/// Holds the data and the codec to the read the data.
#[derive(Clone)]
pub struct FastFieldReaderCodecWrapper<Item: FastValue, CodecReader> {
    reader: CodecReader,
    _phantom: PhantomData<Item>,
}

impl<Item: FastValue, C: FastFieldCodecReader> FastFieldReaderCodecWrapper<Item, C> {
    /// Opens a fast field given a file.
    pub fn open(file: FileSlice) -> crate::Result<Self> {
        let mut bytes = file.read_bytes()?;
        let codec_code = bytes.read_u8();
        let codec_type = FastFieldCodecType::from_code(codec_code).ok_or_else(|| {
            DataCorruption::comment_only("Unknown codec code does not exist `{codec_code}`")
        })?;
        assert_eq!(
            FastFieldCodecType::Bitpacked,
            codec_type,
            "Tried to open fast field as bitpacked encoded (id=1), but got serializer with \
             different id"
        );
        Self::open_from_bytes(bytes)
    }
    /// Opens a fast field given the bytes.
    pub fn open_from_bytes(bytes: OwnedBytes) -> crate::Result<Self> {
        let reader = C::open_from_bytes(bytes)?;
        Ok(FastFieldReaderCodecWrapper {
            reader,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub(crate) fn get_u64(&self, doc: u64) -> Item {
        let data = self.reader.get_u64(doc);
        Item::from_u64(data)
    }

    /// Internally `multivalued` also use SingleValue Fast fields.
    /// It works as follows... A first column contains the list of start index
    /// for each document, a second column contains the actual values.
    ///
    /// The values associated to a given doc, are then
    ///  `second_column[first_column.get(doc)..first_column.get(doc+1)]`.
    ///
    /// Which means single value fast field reader can be indexed internally with
    /// something different from a `DocId`. For this use case, we want to use `u64`
    /// values.
    ///
    /// See `get_range` for an actual documentation about this method.
    pub(crate) fn get_range_u64(&self, start: u64, output: &mut [Item]) {
        for (i, out) in output.iter_mut().enumerate() {
            *out = self.get_u64(start + (i as u64));
        }
    }
}

impl<Item: FastValue, C: FastFieldCodecReader + Clone> FastFieldReader<Item>
    for FastFieldReaderCodecWrapper<Item, C>
{
    /// Return the value associated to the given document.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    // `maxdoc`.
    fn get(&self, doc: DocId) -> Item {
        self.get_u64(u64::from(doc))
    }

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// Regardless of the type of `Item`, this method works
    /// - transmuting the output array
    /// - extracting the `Item`s as if they were `u64`
    /// - possibly converting the `u64` value to the right type.
    ///
    /// # Panics
    ///
    /// May panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    fn get_range(&self, start: u64, output: &mut [Item]) {
        self.get_range_u64(start, output);
    }

    /// Returns the minimum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    fn min_value(&self) -> Item {
        Item::from_u64(self.reader.min_value())
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    fn max_value(&self) -> Item {
        Item::from_u64(self.reader.max_value())
    }
}

impl<Item: FastValue> From<Vec<Item>> for DynamicFastFieldReader<Item> {
    fn from(vals: Vec<Item>) -> DynamicFastFieldReader<Item> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let path = Path::new("__dummy__");
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory
                .open_write(path)
                .expect("With a RamDirectory, this should never fail.");
            let mut serializer = CompositeFastFieldSerializer::from_write(write)
                .expect("With a RamDirectory, this should never fail.");
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            {
                let fast_field_writer = fast_field_writers
                    .get_field_writer_mut(field)
                    .expect("With a RamDirectory, this should never fail.");
                for val in vals {
                    fast_field_writer.add_val(val.to_u64());
                }
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }

        let file = directory.open_read(path).expect("Failed to open the file");
        let composite_file = CompositeFile::open(&file).expect("Failed to read the composite file");
        let field_file = composite_file
            .open_read(field)
            .expect("File component not found");
        DynamicFastFieldReader::open(field_file).unwrap()
    }
}
