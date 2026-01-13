use std::cmp::Ordering;
use std::io::{self, Write as _};

use common::{BinarySerializable as _, VInt};

use crate::codec::postings::PostingsSerializer;
use crate::codec::standard::postings::block::Block;
use crate::codec::standard::postings::skip::SkipSerializer;
use crate::fieldnorm::FieldNormReader;
use crate::postings::compression::{BlockEncoder, VIntEncoder as _, COMPRESSION_BLOCK_SIZE};
use crate::query::Bm25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score};

pub struct StandardPostingsSerializer {
    last_doc_id_encoded: u32,

    block_encoder: BlockEncoder,
    block: Box<Block>,

    postings_write: Vec<u8>,
    skip_write: SkipSerializer,

    mode: IndexRecordOption,
    fieldnorm_reader: Option<FieldNormReader>,

    bm25_weight: Option<Bm25Weight>,
    avg_fieldnorm: Score, /* Average number of term in the field for that segment.
                           * this value is used to compute the block wand information. */
    term_has_freq: bool,
}

impl StandardPostingsSerializer {
    pub fn new(
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> StandardPostingsSerializer {
        Self {
            last_doc_id_encoded: 0,
            block_encoder: BlockEncoder::new(),
            block: Box::new(Block::new()),
            postings_write: Vec::new(),
            skip_write: SkipSerializer::new(),
            mode,
            fieldnorm_reader,
            bm25_weight: None,
            avg_fieldnorm,
            term_has_freq: false,
        }
    }
}

impl PostingsSerializer for StandardPostingsSerializer {
    fn new_term(&mut self, term_doc_freq: u32, record_term_freq: bool) {
        self.bm25_weight = None;

        self.term_has_freq = self.mode.has_freq() && record_term_freq;
        if !self.term_has_freq {
            return;
        }

        let num_docs_in_segment: u64 =
            if let Some(fieldnorm_reader) = self.fieldnorm_reader.as_ref() {
                fieldnorm_reader.num_docs() as u64
            } else {
                return;
            };

        if num_docs_in_segment == 0 {
            return;
        }

        self.bm25_weight = Some(Bm25Weight::for_one_term_without_explain(
            term_doc_freq as u64,
            num_docs_in_segment,
            self.avg_fieldnorm,
        ));
    }

    fn write_doc(&mut self, doc_id: DocId, term_freq: u32) {
        self.block.append_doc(doc_id, term_freq);
        if self.block.is_full() {
            self.write_block();
        }
    }

    fn close_term(
        &mut self,
        doc_freq: u32,
        output_write: &mut impl std::io::Write,
    ) -> io::Result<()> {
        if !self.block.is_empty() {
            // we have doc ids waiting to be written
            // this happens when the number of doc ids is
            // not a perfect multiple of our block size.
            //
            // In that case, the remaining part is encoded
            // using variable int encoding.
            {
                let block_encoded = self
                    .block_encoder
                    .compress_vint_sorted(self.block.doc_ids(), self.last_doc_id_encoded);
                self.postings_write.write_all(block_encoded)?;
            }
            // ... Idem for term frequencies
            if self.term_has_freq {
                let block_encoded = self
                    .block_encoder
                    .compress_vint_unsorted(self.block.term_freqs());
                self.postings_write.write_all(block_encoded)?;
            }
            self.block.clear();
        }
        if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            let skip_data = self.skip_write.data();
            VInt(skip_data.len() as u64).serialize(output_write)?;
            output_write.write_all(skip_data)?;
        }
        output_write.write_all(&self.postings_write[..])?;
        self.skip_write.clear();
        self.postings_write.clear();
        self.bm25_weight = None;
        Ok(())
    }

    fn clear(&mut self) {
        self.block.clear();
        self.last_doc_id_encoded = 0;
    }
}

impl StandardPostingsSerializer {
    fn write_block(&mut self) {
        {
            // encode the doc ids
            let (num_bits, block_encoded): (u8, &[u8]) = self
                .block_encoder
                .compress_block_sorted(self.block.doc_ids(), self.last_doc_id_encoded);
            self.last_doc_id_encoded = self.block.last_doc();
            self.skip_write
                .write_doc(self.last_doc_id_encoded, num_bits);
            // last el block 0, offset block 1,
            self.postings_write.extend(block_encoded);
        }
        if self.term_has_freq {
            let (num_bits, block_encoded): (u8, &[u8]) = self
                .block_encoder
                .compress_block_unsorted(self.block.term_freqs(), true);
            self.postings_write.extend(block_encoded);
            self.skip_write.write_term_freq(num_bits);
            if self.mode.has_positions() {
                // We serialize the sum of term freqs within the skip information
                // in order to navigate through positions.
                let sum_freq = self.block.term_freqs().iter().cloned().sum();
                self.skip_write.write_total_term_freq(sum_freq);
            }
            let mut blockwand_params = (0u8, 0u32);
            if let Some(bm25_weight) = self.bm25_weight.as_ref() {
                if let Some(fieldnorm_reader) = self.fieldnorm_reader.as_ref() {
                    let docs = self.block.doc_ids().iter().cloned();
                    let term_freqs = self.block.term_freqs().iter().cloned();
                    let fieldnorms = docs.map(|doc| fieldnorm_reader.fieldnorm_id(doc));
                    blockwand_params = fieldnorms
                        .zip(term_freqs)
                        .max_by(
                            |(left_fieldnorm_id, left_term_freq),
                             (right_fieldnorm_id, right_term_freq)| {
                                let left_score =
                                    bm25_weight.tf_factor(*left_fieldnorm_id, *left_term_freq);
                                let right_score =
                                    bm25_weight.tf_factor(*right_fieldnorm_id, *right_term_freq);
                                left_score
                                    .partial_cmp(&right_score)
                                    .unwrap_or(Ordering::Equal)
                            },
                        )
                        .unwrap();
                }
            }
            let (fieldnorm_id, term_freq) = blockwand_params;
            self.skip_write.write_blockwand_max(fieldnorm_id, term_freq);
        }
        self.block.clear();
    }
}
