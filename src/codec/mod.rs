pub mod postings;
pub mod standard;

use std::borrow::Cow;
use std::io;

use serde::{Deserialize, Serialize};
pub use standard::StandardCodec;

use crate::codec::postings::PostingsCodec;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{Postings, TermInfo};
use crate::query::{box_scorer, Bm25Weight, Scorer};
use crate::schema::IndexRecordOption;
use crate::{DocId, InvertedIndexReader, Score};

pub trait Codec: Clone + std::fmt::Debug + Send + Sync + 'static {
    type PostingsCodec: PostingsCodec;

    const NAME: &'static str;

    fn from_json_props(json_value: &serde_json::Value) -> crate::Result<Self>;

    fn to_json_props(&self) -> serde_json::Value;

    fn postings_codec(&self) -> &Self::PostingsCodec;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CodecConfiguration {
    name: Cow<'static, str>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    props: serde_json::Value,
}

impl CodecConfiguration {
    pub fn from_codec<C: Codec>(codec: &C) -> Self {
        CodecConfiguration {
            name: Cow::Borrowed(C::NAME),
            props: codec.to_json_props(),
        }
    }

    pub fn to_codec<C: Codec>(&self) -> crate::Result<C> {
        if self.name != C::NAME {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Codec name mismatch: expected {}, got {}",
                C::NAME,
                self.name
            )));
        }
        C::from_json_props(&self.props)
    }
}

impl Default for CodecConfiguration {
    fn default() -> Self {
        CodecConfiguration::from_codec(&StandardCodec)
    }
}

pub trait ObjectSafeCodec: 'static + Send + Sync {
    fn load_postings_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Postings>>;

    fn load_term_scorer_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>>;

    fn new_phrase_scorer_type_erased(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Scorer>>;

    fn try_for_each_pruning(
        &self,
        threshold: Score,
        scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> Result<(), Box<dyn Scorer>>;
}

impl<TCodec: Codec> ObjectSafeCodec for TCodec {
    fn load_postings_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Postings>> {
        let postings = inverted_index_reader
            .read_postings_from_terminfo_specialized(term_info, option, self)?;
        Ok(Box::new(postings))
    }

    fn load_term_scorer_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>> {
        let scorer = inverted_index_reader.new_term_scorer_specialized(
            term_info,
            option,
            fieldnorm_reader,
            similarity_weight,
            self,
        )?;
        Ok(box_scorer(scorer))
    }

    fn new_phrase_scorer_type_erased(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Scorer>> {
        let scorer = inverted_index_reader.new_phrase_scorer_type_specialized(
            term_infos,
            similarity_weight,
            fieldnorm_reader,
            slop,
            self,
        )?;
        Ok(box_scorer(scorer))
    }

    fn try_for_each_pruning(
        &self,
        threshold: Score,
        scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> Result<(), Box<dyn Scorer>> {
        <TCodec as Codec>::PostingsCodec::try_for_each_pruning(threshold, scorer, callback)
    }
}
