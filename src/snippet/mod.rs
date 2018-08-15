use tokenizer::{TokenStream, Tokenizer};
use std::collections::BTreeMap;
use Term;
use Document;
use Index;
use schema::FieldValue;
use schema::Value;
use tokenizer::BoxedTokenizer;

pub struct HighlightSection {
    start: usize,
    stop: usize,
}

impl HighlightSection {
    fn new(start: usize, stop: usize) -> HighlightSection {
        HighlightSection {
            start,
            stop
        }
    }
}

pub struct FragmentCandidate {
    score: f32,
    start_offset: usize,
    stop_offset: usize,
    num_chars: usize,
    highlighted: Vec<HighlightSection>,
}

pub struct Snippet {
    fragments: Vec<String>,
}

impl Snippet {
    pub fn to_html() -> String {
        unimplemented!();
    }
}

/// Returns a non-empty list of "good" fragments.
///
/// If no target term is within the text, then the function
/// should return an empty Vec.
///
/// If a target term is within the text, then the returned
/// list is required to be non-empty.
///
/// The returned list is non-empty and contain less
/// than 12 possibly overlapping fragments.
///
/// All fragments should contain at least one target term
/// and have at most `max_num_chars` characters (not bytes).
///
/// It is ok to emit non-overlapping fragments, for instance,
/// one short and one long containing the same keyword, in order
/// to leave optimization opportunity to the fragment selector
/// upstream.
///
/// Fragments must be valid in the sense that `&text[fragment.start..fragment.stop]`\
/// has to be a valid string.
fn search_fragments<'a>(
    tokenizer: &BoxedTokenizer,
    text: &'a str,
    terms: BTreeMap<String, f32>,
    max_num_chars: usize) -> Vec<FragmentCandidate> {
    unimplemented!();
}

fn select_best_fragment_combination(fragments_candidate: Vec<(&str, Vec<FragmentCandidate>)>, max_num_chars: usize) -> Snippet {
    unimplemented!();
}

pub fn generate_snippet<'a>(
    doc: &'a [FieldValue],
    index: &Index,
    terms: Vec<Term>,
    max_num_chars: usize) -> Snippet {
    unimplemented!();
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_snippet() {}
}