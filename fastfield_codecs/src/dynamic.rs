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
use std::sync::Arc;

use ownedbytes::OwnedBytes;

use crate::FastFieldCodec;
use crate::bitpacked::BitpackedFastFieldSerializer;
use crate::linearinterpol::LinearInterpolFastFieldSerializer;
use crate::FastFieldCodecReader;
use crate::gcd::GCDFastFieldCodecSerializer;
use crate::multilinearinterpol::MultiLinearInterpolFastFieldSerializer;

pub struct DynamicFastFieldSerializer;

impl FastFieldCodec for DynamicFastFieldSerializer {
    const NAME: &'static str = "dynamic";

    type Reader = DynamicFastFieldReader;

    fn is_applicable(fastfield_accessor: &impl crate::FastFieldDataAccess, stats: crate::FastFieldStats) -> bool {
        todo!()
    }

    fn estimate(fastfield_accessor: &impl crate::FastFieldDataAccess, stats: crate::FastFieldStats) -> f32 {
        todo!()
    }

    fn serialize(
        &self,
        write: &mut impl io::Write,
        fastfield_accessor: &dyn crate::FastFieldDataAccess,
        stats: crate::FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        data_iter1: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        todo!()
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
            CodecType::Bitpacked => Arc::new(BitpackedFastFieldSerializer::open_from_bytes(bytes)?),
            CodecType::LinearInterpol => {
                Arc::new(LinearInterpolFastFieldSerializer::open_from_bytes(bytes)?)
            }
            CodecType::MultiLinearInterpol => {
                Arc::new(MultiLinearInterpolFastFieldSerializer::open_from_bytes(bytes)?)
            }
            CodecType::Gcd => {
                let inner_codec_id = bytes.read_u8();
                let inner_codec_type = CodecType::from_code(inner_codec_id).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unknown codec code `{codec_code}`"),
                    )
                })?;
                match inner_codec_type {
                    CodecType::Bitpacked => {
                        Arc::new(GCDFastFieldCodecSerializer::<BitpackedFastFieldSerializer>::open_from_bytes(bytes)?)
                    }
                    CodecType::LinearInterpol => {
                        Arc::new(GCDFastFieldCodecSerializer::<LinearInterpolFastFieldSerializer>::open_from_bytes(bytes)?)
                    }
                    CodecType::MultiLinearInterpol => {
                        Arc::new(GCDFastFieldCodecSerializer::<MultiLinearInterpolFastFieldSerializer>::open_from_bytes(bytes)?)
                    }
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
enum CodecType {
    Bitpacked = 0,
    LinearInterpol = 1,
    MultiLinearInterpol = 2,
    Gcd = 3,
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
