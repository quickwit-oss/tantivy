//! Stemmers for languages the Snowball project does not cover.
//!
//! Tantivy's stemmers normally come from [`frostem`], a Rust code generation of the Snowball
//! algorithms. Snowball has no algorithm for a handful of languages that tantivy ships stop word
//! lists for, so the stemmers in this module are ported from Apache Lucene instead.
//!
//! # Provenance
//!
//! Ported from Apache Lucene, commit `3cee9ed259e9d932b0fa64cb0ec12f4c1e76ef6b`, from
//! `lucene/analysis/common/src/java/org/apache/lucene/analysis/`:
//!
//! | Module | Lucene source |
//! |---|---|
//! | [`bengali`] | `bn/BengaliStemmer.java` |
//! | [`bulgarian`] | `bg/BulgarianStemmer.java` |
//! | [`latvian`] | `lv/LatvianStemmer.java` |
//! | [`sorani`] | `ckb/SoraniStemmer.java` |
//! | [`telugu`] | `te/TeluguStemmer.java` |
//! | [`util`] | `util/StemmerUtil.java` |
//!
//! [`slovak`] is the exception: it comes from Wikimedia's `search-extra` rather than
//! Lucene, under Apache-2.0 and MIT. Its own module documentation has the detail.
//!
//! Lucene is licensed under the Apache License, Version 2.0:
//!
//! ```text
//! Licensed to the Apache Software Foundation (ASF) under one or more
//! contributor license agreements.  See the NOTICE file distributed with
//! this work for additional information regarding copyright ownership.
//! The ASF licenses this file to You under the Apache License, Version 2.0
//! (the "License"); you may not use this file except in compliance with
//! the License.  You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
//! ```
//!
//! # Normalization
//!
//! Lucene's full analyzer chains apply a normalizer before stemming for some of these languages:
//! Bengali and Telugu use `IndicNormalizationFilter` plus a language-specific normalizer, and
//! Sorani uses `SoraniNormalizationFilter`. Bulgarian and Latvian use none.
//!
//! Those normalizers are **not** ported, which has one known consequence worth stating. Lucene's
//! Bulgarian, Latvian, Bengali and Telugu stemmer tests drive the stem filter directly and all of
//! their vectors pass here. `TestSoraniStemFilter`, however, builds a full `SoraniAnalyzer`, and
//! two of its vectors spell kaf as `\u{643}` where [`sorani`]'s suffixes use `\u{6a9}`; without
//! normalization those words are not stemmed. Sorani text uses both spellings in practice, so
//! Sorani in particular benefits from normalizing before the [`Stemmer`] filter. See
//! `tests::SORANI_REQUIRES_NORMALIZATION`.
//!
//! # Porting notes
//!
//! Lucene operates on a `char[]` of UTF-16 code units with an explicit length, returning the new
//! length, and some of these stemmers rewrite characters in place rather than only truncating.
//! Every language here is confined to the Basic Multilingual Plane, so UTF-16 code units and
//! Rust [`char`]s coincide and the ports are faithful — but only because they work on `char`s.
//! Operating on bytes would be wrong: Bengali, Bulgarian, Sorani and Telugu text is two to three
//! bytes per character in UTF-8, so byte indexing would split characters.
//!
//! [`Stemmer`]: super::Stemmer

use std::borrow::Cow;

mod bengali;
mod bulgarian;
mod latvian;
mod slovak;
mod sorani;
mod telugu;
mod util;

#[cfg(test)]
mod tests;

/// A stemmer ported from Apache Lucene, for a language Snowball does not cover.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum LightAlgorithm {
    Bengali,
    Bulgarian,
    Latvian,
    Slovak,
    Sorani,
    Telugu,
}

impl LightAlgorithm {
    /// Stems a single word.
    ///
    /// `scratch` is reused across calls so that stemming a token stream does not allocate a
    /// character buffer per token.
    pub(super) fn stem<'a>(self, word: &'a str, scratch: &mut Vec<char>) -> Cow<'a, str> {
        scratch.clear();
        scratch.extend(word.chars());
        let len = scratch.len();

        let stemmed_len = match self {
            LightAlgorithm::Bengali => bengali::stem(scratch, len),
            LightAlgorithm::Bulgarian => bulgarian::stem(scratch, len),
            LightAlgorithm::Latvian => latvian::stem(scratch, len),
            LightAlgorithm::Slovak => slovak::stem(scratch, len),
            LightAlgorithm::Sorani => sorani::stem(scratch, len),
            LightAlgorithm::Telugu => telugu::stem(scratch, len),
        };
        debug_assert!(stemmed_len <= len, "a stemmer must not lengthen the word");

        // Untouched words are the common case; hand the caller its own string back rather than
        // reallocating. The length alone is not enough, since Bulgarian and Latvian can rewrite
        // characters without shortening the word.
        if stemmed_len == len && scratch.iter().copied().eq(word.chars()) {
            return Cow::Borrowed(word);
        }
        Cow::Owned(scratch[..stemmed_len].iter().collect())
    }
}
