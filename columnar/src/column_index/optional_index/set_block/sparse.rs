use crate::column_index::optional_index::{Set, SetCodec};

pub struct SparseBlockCodec;

impl SetCodec for SparseBlockCodec {
    type Item = u16;
    type Reader<'a> = SparseBlock<'a>;

    fn serialize(
        els: impl Iterator<Item = u16>,
        mut wrt: impl std::io::Write,
    ) -> std::io::Result<()> {
        for el in els {
            wrt.write_all(&el.to_le_bytes())?;
        }
        Ok(())
    }

    fn open<'a>(data: &'a [u8]) -> Self::Reader<'a> {
        SparseBlock(data)
    }
}

#[derive(Copy, Clone)]
pub struct SparseBlock<'a>(&'a [u8]);

impl<'a> Set<u16> for SparseBlock<'a> {
    #[inline(always)]
    fn contains(&self, el: u16) -> bool {
        self.binary_search(el).is_ok()
    }

    #[inline(always)]
    fn rank_if_exists(&self, el: u16) -> Option<u16> {
        self.binary_search(el).ok()
    }

    #[inline(always)]
    fn select(&self, rank: u16) -> u16 {
        let offset = rank as usize * 2;
        u16::from_le_bytes(self.0[offset..offset + 2].try_into().unwrap())
    }

    fn select_batch(&self, ranks: &[u16], outputs: &mut [u16]) {
        let orig_ids = self.select_iter(ranks.iter().copied());
        for (output, original_id) in outputs.iter_mut().zip(orig_ids) {
            *output = original_id;
        }
    }
}

#[inline(always)]
fn get_u16(data: &[u8], byte_position: usize) -> u16 {
    let bytes: [u8; 2] = data[byte_position..byte_position + 2].try_into().unwrap();
    u16::from_le_bytes(bytes)
}

impl<'a> SparseBlock<'a> {
    #[inline(always)]
    fn value_at_idx(&self, data: &[u8], idx: u16) -> u16 {
        let start_offset: usize = idx as usize * 2;
        get_u16(data, start_offset)
    }

    #[inline]
    fn num_vals(&self) -> u16 {
        (self.0.len() / 2) as u16
    }

    #[inline]
    #[allow(clippy::comparison_chain)]
    // Looks for the element in the block. Returns the positions if found.
    fn binary_search(&self, target: u16) -> Result<u16, u16> {
        let data = &self.0;
        let mut size = self.num_vals();
        let mut left = 0;
        let mut right = size;
        // TODO try different implem.
        //  e.g. exponential search into binary search
        while left < right {
            let mid = left + size / 2;

            // TODO do boundary check only once, and then use an
            // unsafe `value_at_idx`
            let mid_val = self.value_at_idx(data, mid);

            if target > mid_val {
                left = mid + 1;
            } else if target < mid_val {
                right = mid;
            } else {
                return Ok(mid);
            }

            size = right - left;
        }
        Err(left)
    }

    pub fn select_iter<'b>(
        &self,
        iter: impl Iterator<Item = u16> + 'b,
    ) -> impl Iterator<Item = u16> + 'b
    where
        Self: 'b,
    {
        iter.map(|codec_id| {
            let offset = codec_id as usize * 2;
            u16::from_le_bytes(self.0[offset..offset + 2].try_into().unwrap())
        })
    }
}
