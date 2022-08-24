use std::io::{self, Write};
use std::num::NonZeroU64;

use common::BinarySerializable;
use fastdivide::DividerU64;
use fastfield_codecs::FastFieldCodecReader;
use ownedbytes::OwnedBytes;

pub const GCD_DEFAULT: u64 = 1;

/// Wrapper for accessing a fastfield.
///
/// Holds the data and the codec to the read the data.
#[derive(Clone)]
pub struct GCDFastFieldCodec<CodecReader> {
    gcd: u64,
    min_value: u64,
    reader: CodecReader,
}

impl<C: FastFieldCodecReader + Clone> FastFieldCodecReader for GCDFastFieldCodec<C> {
    /// Opens a fast field given the bytes.
    fn open_from_bytes(bytes: OwnedBytes) -> std::io::Result<Self> {
        let footer_offset = bytes.len() - 16;
        let (body, mut footer) = bytes.split(footer_offset);
        let gcd = u64::deserialize(&mut footer)?;
        let min_value = u64::deserialize(&mut footer)?;
        let reader = C::open_from_bytes(body)?;
        Ok(GCDFastFieldCodec {
            gcd,
            min_value,
            reader,
        })
    }

    #[inline]
    fn get_u64(&self, doc: u64) -> u64 {
        let mut data = self.reader.get_u64(doc);
        data *= self.gcd;
        data += self.min_value;
        data
    }

    fn min_value(&self) -> u64 {
        self.min_value + self.reader.min_value() * self.gcd
    }

    fn max_value(&self) -> u64 {
        self.min_value + self.reader.max_value() * self.gcd
    }
}

pub fn write_gcd_header<W: Write>(field_write: &mut W, min_value: u64, gcd: u64) -> io::Result<()> {
    gcd.serialize(field_write)?;
    min_value.serialize(field_write)?;
    Ok(())
}

/// Compute the gcd of two non null numbers.
///
/// It is recommended, but not required, to feed values such that `large >= small`.
fn compute_gcd(mut large: NonZeroU64, mut small: NonZeroU64) -> NonZeroU64 {
    loop {
        let rem: u64 = large.get() % small;
        if let Some(new_small) = NonZeroU64::new(rem) {
            (large, small) = (small, new_small);
        } else {
            return small;
        }
    }
}

// Find GCD for iterator of numbers
pub fn find_gcd(numbers: impl Iterator<Item = u64>) -> Option<NonZeroU64> {
    let mut numbers = numbers.flat_map(NonZeroU64::new);
    let mut gcd: NonZeroU64 = numbers.next()?;
    if gcd.get() == 1 {
        return Some(gcd);
    }

    let mut gcd_divider = DividerU64::divide_by(gcd.get());
    for val in numbers {
        let remainder = val.get() - (gcd_divider.divide(val.get())) * gcd.get();
        if remainder == 0 {
            continue;
        }
        gcd = compute_gcd(val, gcd);
        if gcd.get() == 1 {
            return Some(gcd);
        }

        gcd_divider = DividerU64::divide_by(gcd.get());
    }
    Some(gcd)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use common::HasLen;

    use crate::directory::{CompositeFile, RamDirectory, WritePtr};
    use crate::fastfield::gcd::compute_gcd;
    use crate::fastfield::serializer::FastFieldCodecEnableCheck;
    use crate::fastfield::tests::{FIELD, FIELDI64, SCHEMA, SCHEMAI64};
    use crate::fastfield::{
        find_gcd, CompositeFastFieldSerializer, DynamicFastFieldReader, FastFieldCodecType,
        FastFieldReader, FastFieldsWriter, ALL_CODECS,
    };
    use crate::schema::{Cardinality, Schema};
    use crate::{DateOptions, DatePrecision, DateTime, Directory};

    fn get_index(
        docs: &[crate::Document],
        schema: &Schema,
        codec_enable_checker: FastFieldCodecEnableCheck,
    ) -> crate::Result<RamDirectory> {
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer =
                CompositeFastFieldSerializer::from_write_with_codec(write, codec_enable_checker)
                    .unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(schema);
            for doc in docs {
                fast_field_writers.add_document(doc);
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        Ok(directory)
    }

    fn test_fastfield_gcd_i64_with_codec(
        code_type: FastFieldCodecType,
        num_vals: usize,
    ) -> crate::Result<()> {
        let path = Path::new("test");
        let mut docs = vec![];
        for i in 1..=num_vals {
            let val = (i as i64 - 5) * 1000i64;
            docs.push(doc!(*FIELDI64=>val));
        }
        let directory = get_index(&docs, &SCHEMAI64, code_type.clone().into())?;
        let file = directory.open_read(path).unwrap();
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<i64>::open(file)?;

        assert_eq!(fast_field_reader.get(0), -4000i64);
        assert_eq!(fast_field_reader.get(1), -3000i64);
        assert_eq!(fast_field_reader.get(2), -2000i64);
        assert_eq!(fast_field_reader.max_value(), (num_vals as i64 - 5) * 1000);
        assert_eq!(fast_field_reader.min_value(), -4000i64);
        let file = directory.open_read(path).unwrap();

        // Can't apply gcd
        let path = Path::new("test");
        docs.pop();
        docs.push(doc!(*FIELDI64=>2001i64));
        let directory = get_index(&docs, &SCHEMAI64, code_type.into())?;
        let file2 = directory.open_read(path).unwrap();
        assert!(file2.len() > file.len());

        Ok(())
    }

    #[test]
    fn test_fastfield_gcd_i64() -> crate::Result<()> {
        for code_type in ALL_CODECS {
            test_fastfield_gcd_i64_with_codec(code_type.clone(), 5005)?;
        }
        Ok(())
    }

    fn test_fastfield_gcd_u64_with_codec(
        code_type: FastFieldCodecType,
        num_vals: usize,
    ) -> crate::Result<()> {
        let path = Path::new("test");
        let mut docs = vec![];
        for i in 1..=num_vals {
            let val = i as u64 * 1000u64;
            docs.push(doc!(*FIELD=>val));
        }
        let directory = get_index(&docs, &SCHEMA, code_type.clone().into())?;
        let file = directory.open_read(path).unwrap();
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<u64>::open(file)?;
        assert_eq!(fast_field_reader.get(0), 1000u64);
        assert_eq!(fast_field_reader.get(1), 2000u64);
        assert_eq!(fast_field_reader.get(2), 3000u64);
        assert_eq!(fast_field_reader.max_value(), num_vals as u64 * 1000);
        assert_eq!(fast_field_reader.min_value(), 1000u64);
        let file = directory.open_read(path).unwrap();

        // Can't apply gcd
        let path = Path::new("test");
        docs.pop();
        docs.push(doc!(*FIELDI64=>2001u64));
        let directory = get_index(&docs, &SCHEMA, code_type.into())?;
        let file2 = directory.open_read(path).unwrap();
        assert!(file2.len() > file.len());

        Ok(())
    }

    #[test]
    fn test_fastfield_gcd_u64() -> crate::Result<()> {
        for code_type in ALL_CODECS {
            test_fastfield_gcd_u64_with_codec(code_type.clone(), 5005)?;
        }
        Ok(())
    }

    #[test]
    pub fn test_fastfield2() {
        let test_fastfield = DynamicFastFieldReader::<u64>::from(vec![100, 200, 300]);
        assert_eq!(test_fastfield.get(0), 100);
        assert_eq!(test_fastfield.get(1), 200);
        assert_eq!(test_fastfield.get(2), 300);
    }

    #[test]
    pub fn test_gcd_date() -> crate::Result<()> {
        let size_prec_sec =
            test_gcd_date_with_codec(FastFieldCodecType::Bitpacked, DatePrecision::Seconds)?;
        let size_prec_micro =
            test_gcd_date_with_codec(FastFieldCodecType::Bitpacked, DatePrecision::Microseconds)?;
        assert!(size_prec_sec < size_prec_micro);

        let size_prec_sec =
            test_gcd_date_with_codec(FastFieldCodecType::LinearInterpol, DatePrecision::Seconds)?;
        let size_prec_micro = test_gcd_date_with_codec(
            FastFieldCodecType::LinearInterpol,
            DatePrecision::Microseconds,
        )?;
        assert!(size_prec_sec < size_prec_micro);

        Ok(())
    }

    fn test_gcd_date_with_codec(
        codec_type: FastFieldCodecType,
        precision: DatePrecision,
    ) -> crate::Result<usize> {
        let time1 = DateTime::from_timestamp_micros(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        );
        let time2 = DateTime::from_timestamp_micros(
            SystemTime::now()
                .checked_sub(Duration::from_micros(4111))
                .unwrap()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        );

        let time3 = DateTime::from_timestamp_micros(
            SystemTime::now()
                .checked_sub(Duration::from_millis(2000))
                .unwrap()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        );

        let mut schema_builder = Schema::builder();
        let date_options = DateOptions::default()
            .set_fast(Cardinality::SingleValue)
            .set_precision(precision);
        let field = schema_builder.add_date_field("field", date_options);
        let schema = schema_builder.build();

        let docs = vec![doc!(field=>time1), doc!(field=>time2), doc!(field=>time3)];

        let directory = get_index(&docs, &schema, codec_type.into())?;
        let path = Path::new("test");
        let file = directory.open_read(path).unwrap();
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let len = file.len();
        let test_fastfield = DynamicFastFieldReader::<DateTime>::open(file)?;

        assert_eq!(test_fastfield.get(0), time1.truncate(precision));
        assert_eq!(test_fastfield.get(1), time2.truncate(precision));
        assert_eq!(test_fastfield.get(2), time3.truncate(precision));
        Ok(len)
    }

    #[test]
    fn test_compute_gcd() {
        let test_compute_gcd_aux = |large, small, expected| {
            let large = NonZeroU64::new(large).unwrap();
            let small = NonZeroU64::new(small).unwrap();
            let expected = NonZeroU64::new(expected).unwrap();
            assert_eq!(compute_gcd(small, large), expected);
            assert_eq!(compute_gcd(large, small), expected);
        };
        test_compute_gcd_aux(1, 4, 1);
        test_compute_gcd_aux(2, 4, 2);
        test_compute_gcd_aux(10, 25, 5);
        test_compute_gcd_aux(25, 25, 25);
    }

    #[test]
    fn find_gcd_test() {
        assert_eq!(find_gcd([0].into_iter()), None);
        assert_eq!(find_gcd([0, 10].into_iter()), NonZeroU64::new(10));
        assert_eq!(find_gcd([10, 0].into_iter()), NonZeroU64::new(10));
        assert_eq!(find_gcd([].into_iter()), None);
        assert_eq!(find_gcd([15, 30, 5, 10].into_iter()), NonZeroU64::new(5));
        assert_eq!(find_gcd([15, 16, 10].into_iter()), NonZeroU64::new(1));
        assert_eq!(find_gcd([0, 5, 5, 5].into_iter()), NonZeroU64::new(5));
        assert_eq!(find_gcd([0, 0].into_iter()), None);
    }
}
