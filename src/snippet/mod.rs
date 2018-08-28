use htmlescape::encode_minimal;
use schema::FieldValue;
use schema::Value;
use std::collections::BTreeMap;
use tokenizer::BoxedTokenizer;
use tokenizer::{Token, TokenStream, Tokenizer};
use Document;
use Index;
use Term;

#[derive(Debug)]
pub struct HighlightSection {
    start: usize,
    stop: usize,
}

impl HighlightSection {
    fn new(start: usize, stop: usize) -> HighlightSection {
        HighlightSection { start, stop }
    }
}

#[derive(Debug)]
pub struct FragmentCandidate {
    score: f32,
    start_offset: usize,
    stop_offset: usize,
    num_chars: usize,
    highlighted: Vec<HighlightSection>,
}

impl FragmentCandidate {
    /// Create a basic `FragmentCandidate`
    ///
    /// `score`, `num_chars` are set to 0
    /// and `highlighted` is set to empty vec
    /// stop_offset is set to start_offset, which is taken as a param.
    fn new(start_offset: usize) -> FragmentCandidate {
        FragmentCandidate {
            score: 0.0,
            start_offset: start_offset,
            stop_offset: start_offset,
            num_chars: 0,
            highlighted: vec![],
        }
    }

    /// Updates `score` and `highlighted` fields of the objects.
    ///
    /// taking the token and terms, the token is added to the fragment.
    /// if the token is one of the terms, the score
    /// and highlighted fields are updated in the fragment.
    fn try_add_token(&mut self, token: &Token, terms: &BTreeMap<String, f32>) {
        self.stop_offset = token.offset_to;

        if let Some(score) = terms.get(&token.text.to_lowercase()) {
            self.score += score;
            self.highlighted
                .push(HighlightSection::new(token.offset_from, token.offset_to));
        }
    }
}

#[derive(Debug)]
pub struct Snippet {
    fragments: String,
    highlighted: Vec<HighlightSection>,
}

const HIGHLIGHTEN_PREFIX: &str = "<b>";
const HIGHLIGHTEN_POSTFIX: &str = "</b>";

impl Snippet {
    /// Returns a hignlightned html from the `Snippet`.
    pub fn to_html(&self) -> String {
        let mut html = String::new();
        let mut start_from: usize = 0;

        for item in self.highlighted.iter() {
            html.push_str(&encode_minimal(&self.fragments[start_from..item.start]));
            html.push_str(HIGHLIGHTEN_PREFIX);
            html.push_str(&encode_minimal(&self.fragments[item.start..item.stop]));
            html.push_str(HIGHLIGHTEN_POSTFIX);
            start_from = item.stop;
        }
        html.push_str(&encode_minimal(
            &self.fragments[start_from..self.fragments.len()],
        ));
        html
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
    tokenizer: Box<BoxedTokenizer>,
    text: &'a str,
    terms: BTreeMap<String, f32>,
    max_num_chars: usize,
) -> Vec<FragmentCandidate> {
    let mut token_stream = tokenizer.token_stream(text);
    let mut fragment = FragmentCandidate::new(0);
    let mut fragments: Vec<FragmentCandidate> = vec![];

    while let Some(next) = token_stream.next() {
        if (next.offset_to - fragment.start_offset) > max_num_chars {
            if fragment.score > 0.0 {
                fragments.push(fragment)
            };
            fragment = FragmentCandidate::new(next.offset_from);
        }
        fragment.try_add_token(next, &terms);
    }
    if fragment.score > 0.0 {
        fragments.push(fragment)
    }

    fragments
}

/// Returns a Snippet
///
/// Takes a vector of `FragmentCandidate`s and the text.
/// Figures out the best fragment from it and creates a snippet.
fn select_best_fragment_combination<'a>(
    fragments: Vec<FragmentCandidate>,
    text: &'a str,
) -> Snippet {
    if let Some(init) = fragments.iter().nth(0) {
        let fragment =
            fragments.iter().skip(1).fold(
                init,
                |acc, item| {
                    if item.score > acc.score {
                        item
                    } else {
                        acc
                    }
                },
            );
        let fragment_text = &text[fragment.start_offset..fragment.stop_offset];
        let highlighted = fragment
            .highlighted
            .iter()
            .map(|item| {
                HighlightSection::new(
                    item.start - fragment.start_offset,
                    item.stop - fragment.start_offset,
                )
            }).collect();
        Snippet {
            fragments: fragment_text.to_owned(),
            highlighted: highlighted,
        }
    } else {
        // when there no fragments to chose from,
        // for now create a empty snippet
        Snippet {
            fragments: String::new(),
            highlighted: vec![],
        }
    }
}

pub fn generate_snippet<'a>(
    doc: &'a [FieldValue],
    index: &Index,
    terms: Vec<Term>,
    max_num_chars: usize,
) -> Snippet {
    unimplemented!();
}

#[cfg(test)]
mod tests {
    use super::{search_fragments, select_best_fragment_combination};
    use std::collections::BTreeMap;
    use std::iter::Iterator;
    use tokenizer::{box_tokenizer, SimpleTokenizer};

    const TOKENIZER: SimpleTokenizer = SimpleTokenizer;

    #[test]
    fn test_snippet() {
        let boxed_tokenizer = box_tokenizer(TOKENIZER);

        let text = "Rust is a systems programming language sponsored by Mozilla which describes it as a \"safe, concurrent, practical language\", supporting functional and imperative-procedural paradigms. Rust is syntactically similar to C++[according to whom?], but its designers intend it to provide better memory safety while still maintaining performance.

Rust is free and open-source software, released under an MIT License, or Apache License 2.0. Its designers have refined the language through the experiences of writing the Servo web browser layout engine[14] and the Rust compiler. A large proportion of current commits to the project are from community members.[15]

Rust won first place for \"most loved programming language\" in the Stack Overflow Developer Survey in 2016, 2017, and 2018.
";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);
        terms.insert(String::from("language"), 0.9);

        let fragments = search_fragments(boxed_tokenizer, &text, terms, 100);
        assert_eq!(fragments.len(), 7);
        {
            let first = fragments.iter().nth(0).unwrap();
            assert_eq!(first.score, 1.9);
            assert_eq!(first.stop_offset, 89);
        }
        let snippet = select_best_fragment_combination(fragments, &text);
        assert_eq!(snippet.fragments, "Rust is a systems programming language sponsored by Mozilla which describes it as a \"safe".to_owned());
        assert_eq!(snippet.to_html(), "<b>Rust</b> is a systems programming <b>language</b> sponsored by Mozilla which describes it as a &quot;safe".to_owned())
    }

    #[test]
    fn test_snippet_in_second_fragment() {
        let boxed_tokenizer = box_tokenizer(TOKENIZER);

        let text = "a b c d e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("c"), 1.0);

        let fragments = search_fragments(boxed_tokenizer, &text, terms, 3);

        assert_eq!(fragments.len(), 1);
        {
            let first = fragments.iter().nth(0).unwrap();
            assert_eq!(first.score, 1.0);
            assert_eq!(first.start_offset, 4);
            assert_eq!(first.stop_offset, 7);
        }

        let snippet = select_best_fragment_combination(fragments, &text);
        assert_eq!(snippet.fragments, "c d");
        assert_eq!(snippet.to_html(), "<b>c</b> d");
    }

    #[test]
    fn test_snippet_with_term_at_the_end_of_fragment() {
        let boxed_tokenizer = box_tokenizer(TOKENIZER);

        let text = "a b c d e f f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("f"), 1.0);

        let fragments = search_fragments(boxed_tokenizer, &text, terms, 3);

        assert_eq!(fragments.len(), 2);
        {
            let first = fragments.iter().nth(0).unwrap();
            assert_eq!(first.score, 1.0);
            assert_eq!(first.stop_offset, 11);
            assert_eq!(first.start_offset, 8);
        }

        let snippet = select_best_fragment_combination(fragments, &text);
        assert_eq!(snippet.fragments, "e f");
        assert_eq!(snippet.to_html(), "e <b>f</b>");
    }

    #[test]
    fn test_snippet_with_second_fragment_has_the_highest_score() {
        let boxed_tokenizer = box_tokenizer(TOKENIZER);

        let text = "a b c d e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("f"), 1.0);
        terms.insert(String::from("a"), 0.9);

        let fragments = search_fragments(boxed_tokenizer, &text, terms, 7);

        assert_eq!(fragments.len(), 2);
        {
            let first = fragments.iter().nth(0).unwrap();
            assert_eq!(first.score, 0.9);
            assert_eq!(first.stop_offset, 7);
            assert_eq!(first.start_offset, 0);
        }

        let snippet = select_best_fragment_combination(fragments, &text);
        assert_eq!(snippet.fragments, "e f g");
        assert_eq!(snippet.to_html(), "e <b>f</b> g");
    }

    #[test]
    fn test_snippet_with_term_not_in_text() {
        let boxed_tokenizer = box_tokenizer(TOKENIZER);

        let text = "a b c d";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("z"), 1.0);

        let fragments = search_fragments(boxed_tokenizer, &text, terms, 3);

        assert_eq!(fragments.len(), 0);

        let snippet = select_best_fragment_combination(fragments, &text);
        assert_eq!(snippet.fragments, "");
        assert_eq!(snippet.to_html(), "");
    }

    #[test]
    fn test_snippet_with_no_terms() {
        let boxed_tokenizer = box_tokenizer(TOKENIZER);

        let text = "a b c d";

        let mut terms = BTreeMap::new();
        let fragments = search_fragments(boxed_tokenizer, &text, terms, 3);
        assert_eq!(fragments.len(), 0);

        let snippet = select_best_fragment_combination(fragments, &text);
        assert_eq!(snippet.fragments, "");
        assert_eq!(snippet.to_html(), "");
    }
}
