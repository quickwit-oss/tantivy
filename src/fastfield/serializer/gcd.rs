use fastfield_codecs::{FastFieldDataAccess, FastFieldStats};
use gcd::Gcd;
use libdivide::Divider;

pub const GCD_DEFAULT: u64 = 1;

fn compute_gcd(vals: &[u64], base: u64) -> u64 {
    let mut gcd = (vals[0] - base).gcd(vals[1] - base);

    for el in vals.iter().map(|el| el - base) {
        gcd = gcd.gcd(el);
    }
    gcd
}

fn is_valid_gcd(vals: impl Iterator<Item = u64>, divider: u64, base: u64) -> bool {
    if divider <= 1 {
        return false;
    }
    let d = Divider::new(divider).unwrap(); // this is slow

    for val in vals {
        let val = val - base;
        if val != (val / &d) * divider {
            return false;
        }
    }
    true
}

fn get_samples(fastfield_accessor: &impl FastFieldDataAccess, stats: &FastFieldStats) -> Vec<u64> {
    // let's sample at 0%, 5%, 10% .. 95%, 100%
    let num_samples = stats.num_vals.min(20);
    let step_size = 100.0 / num_samples as f32;
    let mut sample_values = (0..num_samples)
        .map(|idx| (idx as f32 * step_size / 100.0 * stats.num_vals as f32) as usize)
        .map(|pos| fastfield_accessor.get_val(pos as u64))
        .collect::<Vec<_>>();

    sample_values.push(stats.min_value);
    sample_values.push(stats.max_value);
    sample_values
}

pub(crate) fn find_gcd_from_samples(
    samples: &[u64],
    vals: impl Iterator<Item = u64>,
    base: u64,
) -> Option<u64> {
    let estimate_gcd = compute_gcd(samples, base);
    if is_valid_gcd(vals, estimate_gcd, base) {
        Some(estimate_gcd)
    } else {
        None
    }
}

pub(crate) fn find_gcd(
    fastfield_accessor: &impl FastFieldDataAccess,
    stats: FastFieldStats,
    vals: impl Iterator<Item = u64>,
) -> Option<u64> {
    if stats.num_vals == 0 {
        return None;
    }

    let samples = get_samples(fastfield_accessor, &stats);
    find_gcd_from_samples(&samples, vals, stats.min_value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use common::HasLen;

    use super::*;
    use crate::directory::{CompositeFile, RamDirectory, WritePtr};
    use crate::fastfield::serializer::{FastFieldCodecEnableCheck, FastFieldCodecName, ALL_CODECS};
    use crate::fastfield::tests::{FIELD, FIELDI64, SCHEMA, SCHEMAI64};
    use crate::fastfield::{
        CompositeFastFieldSerializer, DynamicFastFieldReader, FastFieldReader, FastFieldsWriter,
    };
    use crate::schema::Schema;
    use crate::Directory;

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
        codec_name: FastFieldCodecName,
        num_vals: usize,
    ) -> crate::Result<()> {
        let path = Path::new("test");
        let mut docs = vec![];
        for i in 1..=num_vals {
            let val = i as i64 * 1000i64;
            docs.push(doc!(*FIELDI64=>val));
        }
        let directory = get_index(&docs, &SCHEMAI64, codec_name.clone().into())?;
        let file = directory.open_read(path).unwrap();
        // assert_eq!(file.len(), 118);
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<i64>::open(file)?;
        assert_eq!(fast_field_reader.get(0), 1000i64);
        assert_eq!(fast_field_reader.get(1), 2000i64);
        assert_eq!(fast_field_reader.get(2), 3000i64);
        assert_eq!(fast_field_reader.max_value(), num_vals as i64 * 1000);
        assert_eq!(fast_field_reader.min_value(), 1000i64);
        let file = directory.open_read(path).unwrap();

        // Can't apply gcd
        let path = Path::new("test");
        docs.pop();
        docs.push(doc!(*FIELDI64=>2001i64));
        let directory = get_index(&docs, &SCHEMAI64, codec_name.into())?;
        let file2 = directory.open_read(path).unwrap();
        assert!(file2.len() > file.len());

        Ok(())
    }

    #[test]
    fn test_fastfield_gcd_i64() -> crate::Result<()> {
        for codec_name in ALL_CODECS {
            test_fastfield_gcd_i64_with_codec(codec_name.clone(), 5005)?;
        }
        Ok(())
    }

    fn test_fastfield_gcd_u64_with_codec(
        codec_name: FastFieldCodecName,
        num_vals: usize,
    ) -> crate::Result<()> {
        let path = Path::new("test");
        let mut docs = vec![];
        for i in 1..=num_vals {
            let val = i as u64 * 1000u64;
            docs.push(doc!(*FIELD=>val));
        }
        let directory = get_index(&docs, &SCHEMA, codec_name.clone().into())?;
        let file = directory.open_read(path).unwrap();
        // assert_eq!(file.len(), 118);
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
        let directory = get_index(&docs, &SCHEMA, codec_name.into())?;
        let file2 = directory.open_read(path).unwrap();
        assert!(file2.len() > file.len());

        Ok(())
    }

    #[test]
    fn test_fastfield_gcd_u64() -> crate::Result<()> {
        for codec_name in ALL_CODECS {
            test_fastfield_gcd_u64_with_codec(codec_name.clone(), 5005)?;
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
    fn test_gcd() {
        let data = vec![
            9223372036854775808_u64,
            9223372036854775808,
            9223372036854775808,
        ];

        let gcd = find_gcd_from_samples(&data, data.iter().cloned(), *data.iter().min().unwrap());
        assert_eq!(gcd, None);
    }

    #[test]
    fn test_gcd2() {
        let data = vec![
            9223372036854775808_u64,
            9223372036854776808,
            9223372036854777808,
        ];

        let gcd = find_gcd_from_samples(&data, data.iter().cloned(), *data.iter().min().unwrap());
        assert_eq!(gcd, Some(1000));
    }
}
