#![no_main]

//! Fuzzes tantivy's tokenizers and token filters against adversarial Unicode.
//!
//! `Token` documents an invariant the rest of the engine depends on: the text
//! that produced a token is `&text[offset_from..offset_to]`, and token filters
//! must not modify offsets. An offset that runs past the end of the input, or
//! that lands inside a multi-byte UTF-8 sequence, makes that slicing panic in
//! downstream code — so rather than merely checking that tokenizing does not
//! crash, this target asserts the invariant itself on every token.
//!
//! Text reaches tokenizers straight from indexed documents and from query
//! strings, so it is fully attacker-controlled.
//!
//! `FacetTokenizer` is deliberately absent: it expects facet-encoded input
//! rather than free text, so feeding it fuzzer strings would report contract
//! violations as crashes.

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter,
    SimpleTokenizer, SplitCompoundWords, TextAnalyzer, TokenStream, Tokenizer, WhitespaceTokenizer,
};

/// Drains `stream`, checking the offset invariants documented on `Token`.
fn check_offsets<T: TokenStream>(text: &str, stream: &mut T) {
    let len = text.len();
    while stream.advance() {
        let token = stream.token();
        let (from, to) = (token.offset_from, token.offset_to);
        assert!(from <= to, "inverted offsets {from}..{to} for {text:?}");
        assert!(
            to <= len,
            "offset_to {to} past end of {len}-byte input {text:?}"
        );
        assert!(
            text.is_char_boundary(from),
            "offset_from {from} splits a codepoint in {text:?}"
        );
        assert!(
            text.is_char_boundary(to),
            "offset_to {to} splits a codepoint in {text:?}"
        );
    }
}

/// Built once: the Aho-Corasick automaton is costly to construct, and its
/// dictionary is ours rather than the fuzzer's.
fn compound_splitter() -> &'static SplitCompoundWords {
    static SPLITTER: OnceLock<SplitCompoundWords> = OnceLock::new();
    SPLITTER.get_or_init(|| {
        SplitCompoundWords::from_dictionary(["dampf", "schiff", "fahrt"])
            .expect("the static dictionary is valid")
    })
}

fuzz_target!(|data: &str| {
    // A conventional indexing pipeline.
    let mut simple = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build();
    check_offsets(data, &mut simple.token_stream(data));

    let mut whitespace = TextAnalyzer::builder(WhitespaceTokenizer::default())
        .filter(LowerCaser)
        .build();
    check_offsets(data, &mut whitespace.token_stream(data));

    // Emits the whole input as one token, which must still span it exactly.
    let mut raw = TextAnalyzer::from(RawTokenizer::default());
    check_offsets(data, &mut raw.token_stream(data));

    // Folding rewrites token text; the offsets must stay tied to the source.
    let mut folded = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(AsciiFoldingFilter)
        .build();
    check_offsets(data, &mut folded.token_stream(data));

    // Cuts tokens at dictionary matches, so it does its own offset arithmetic.
    let mut compound = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(compound_splitter().clone())
        .build();
    check_offsets(data, &mut compound.token_stream(data));

    // Ngrams slice the input at character boundaries. The parameters are fixed
    // and valid, so the constructor cannot fail on fuzzer input.
    let mut ngram = NgramTokenizer::new(2, 3, false).expect("2..=3 is a valid ngram range");
    check_offsets(data, &mut ngram.token_stream(data));
});
