// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;

use common::BinarySerializable;
use fastdivide::DividerU64;
use ownedbytes::OwnedBytes;

use crate::bitpacked::BitpackedFastFieldCodec;
use crate::gcd::{find_gcd, GCDFastFieldCodecReader, GCDParams};
use crate::linearinterpol::LinearInterpolCodec;
use crate::multilinearinterpol::MultiLinearInterpolFastFieldCodec;
use crate::{FastFieldCodec, FastFieldCodecReader, FastFieldStats};

pub struct DynamicFastFieldCodec;

impl FastFieldCodec for DynamicFastFieldCodec {
    const NAME: &'static str = "dynamic";

    type Reader = DynamicFastFieldReader;

    fn is_applicable(_vals: &[u64], _stats: crate::FastFieldStats) -> bool {
        true
    }

    fn estimate(_vals: &[u64], _stats: crate::FastFieldStats) -> f32 {
        0f32
    }

    fn serialize(
        &self,
        wrt: &mut impl io::Write,
        vals: &[u64],
        stats: crate::FastFieldStats,
    ) -> io::Result<()> {
        let gcd: NonZeroU64 = find_gcd(vals.iter().copied().map(|val| val - stats.min_value))
            .unwrap_or(unsafe { NonZeroU64::new_unchecked(1) });
        if gcd.get() > 1 {
            let gcd_divider = DividerU64::divide_by(gcd.get());
            let scaled_vals: Vec<u64> = vals
                .iter()
                .copied()
                .map(|val| gcd_divider.divide(val - stats.min_value))
                .collect();
            <CodecType as BinarySerializable>::serialize(&CodecType::Gcd, wrt)?;
            let gcd_params = GCDParams {
                min_value: stats.min_value,
                gcd,
            };
            gcd_params.serialize(wrt)?;
            let codec_type = choose_codec(stats, &scaled_vals);
            <CodecType as BinarySerializable>::serialize(&codec_type, wrt)?;
            let scaled_stats = FastFieldStats::compute(&scaled_vals);
            codec_type.serialize(wrt, &scaled_vals, scaled_stats)?;
        } else {
            let codec_type = choose_codec(stats, vals);
            wrt.write_all(&[codec_type.to_code()])?;
            codec_type.serialize(wrt, vals, stats)?;
        }
        Ok(())
    }

    fn open_from_bytes(mut bytes: OwnedBytes) -> io::Result<Self::Reader> {
        let codec_code = bytes.read_u8();
        let codec_type = CodecType::from_code(codec_code).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown codec code `{codec_code}`"),
            )
        })?;
        let fast_field_reader: Arc<dyn FastFieldCodecReader> = match codec_type {
            CodecType::Bitpacked => Arc::new(BitpackedFastFieldCodec::open_from_bytes(bytes)?),
            CodecType::LinearInterpol => Arc::new(LinearInterpolCodec::open_from_bytes(bytes)?),
            CodecType::MultiLinearInterpol => {
                Arc::new(MultiLinearInterpolFastFieldCodec::open_from_bytes(bytes)?)
            }
            CodecType::Gcd => {
                let gcd_params = GCDParams::deserialize(&mut bytes)?;
                let inner_codec_type = <CodecType as BinarySerializable>::deserialize(&mut bytes)?;
                match inner_codec_type {
                    CodecType::Bitpacked => Arc::new(GCDFastFieldCodecReader {
                        params: gcd_params,
                        reader: BitpackedFastFieldCodec::open_from_bytes(bytes)?,
                    }),
                    CodecType::LinearInterpol => Arc::new(GCDFastFieldCodecReader {
                        params: gcd_params,
                        reader: LinearInterpolCodec::open_from_bytes(bytes)?,
                    }),
                    CodecType::MultiLinearInterpol => Arc::new(GCDFastFieldCodecReader {
                        params: gcd_params,
                        reader: MultiLinearInterpolFastFieldCodec::open_from_bytes(bytes)?,
                    }),
                    CodecType::Gcd => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "A GCD codec may not wrap another GCD codec.",
                        ));
                    }
                }
            }
        };
        Ok(DynamicFastFieldReader(fast_field_reader))
    }
}

#[derive(Clone)]
/// DynamicFastFieldReader wraps different readers to access
/// the various encoded fastfield data
pub struct DynamicFastFieldReader(Arc<dyn FastFieldCodecReader>);

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum CodecType {
    Bitpacked = 0,
    LinearInterpol = 1,
    MultiLinearInterpol = 2,
    Gcd = 3,
}

impl BinarySerializable for CodecType {
    fn serialize<W: io::Write>(&self, wrt: &mut W) -> io::Result<()> {
        wrt.write_all(&[self.to_code()])?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let codec_code = u8::deserialize(reader)?;
        let codec_type = CodecType::from_code(codec_code).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid codec type code {codec_code}"),
            )
        })?;
        Ok(codec_type)
    }
}

impl CodecType {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(CodecType::Bitpacked),
            1 => Some(CodecType::LinearInterpol),
            2 => Some(CodecType::MultiLinearInterpol),
            3 => Some(CodecType::Gcd),
            _ => None,
        }
    }

    pub fn to_code(self) -> u8 {
        self as u8
    }

    fn codec_estimation(
        &self,
        stats: FastFieldStats,
        vals: &[u64],
        estimations: &mut Vec<(f32, CodecType)>,
    ) {
        let estimate_opt: Option<f32> = match self {
            CodecType::Bitpacked => codec_estimation::<BitpackedFastFieldCodec>(stats, vals),
            CodecType::LinearInterpol => codec_estimation::<LinearInterpolCodec>(stats, vals),
            CodecType::MultiLinearInterpol => {
                codec_estimation::<MultiLinearInterpolFastFieldCodec>(stats, vals)
            }
            CodecType::Gcd => None,
        };
        if let Some(estimate) = estimate_opt {
            if !estimate.is_nan() && estimate.is_finite() {
                estimations.push((estimate, *self));
            }
        }
    }

    fn serialize(
        &self,
        wrt: &mut impl io::Write,
        fastfield_accessor: &[u64],
        stats: FastFieldStats,
    ) -> io::Result<()> {
        match self {
            CodecType::Bitpacked => {
                BitpackedFastFieldCodec.serialize(wrt, fastfield_accessor, stats)?;
            }
            CodecType::LinearInterpol => {
                LinearInterpolCodec.serialize(wrt, fastfield_accessor, stats)?;
            }
            CodecType::MultiLinearInterpol => {
                MultiLinearInterpolFastFieldCodec.serialize(wrt, fastfield_accessor, stats)?;
            }
            CodecType::Gcd => {
                panic!("GCD should never be called that way.");
            }
        }
        Ok(())
    }
}

impl FastFieldCodecReader for DynamicFastFieldReader {
    fn get_u64(&self, doc: u64) -> u64 {
        self.0.get_u64(doc)
    }

    fn min_value(&self) -> u64 {
        self.0.min_value()
    }

    fn max_value(&self) -> u64 {
        self.0.max_value()
    }
}

fn codec_estimation<T: FastFieldCodec>(stats: FastFieldStats, vals: &[u64]) -> Option<f32> {
    if !T::is_applicable(vals, stats.clone()) {
        return None;
    }
    let ratio = T::estimate(vals, stats);
    Some(ratio)
}

const CODEC_TYPES: [CodecType; 3] = [
    CodecType::Bitpacked,
    CodecType::LinearInterpol,
    CodecType::MultiLinearInterpol,
];

fn choose_codec(stats: FastFieldStats, vals: &[u64]) -> CodecType {
    let mut estimations = Vec::new();
    for codec_type in &CODEC_TYPES {
        codec_type.codec_estimation(stats, vals, &mut estimations);
    }
    estimations.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let (_ratio, codec_type) = estimations[0];
    codec_type
}
