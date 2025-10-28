//! [`SnippetGenerator`]
//! Generates a text snippet for a given document, and some highlighted parts inside it.
//!
//! Imagine you doing a text search in a document
//! and want to show a preview of where in the document the search terms occur,
//! along with some surrounding text to give context, and the search terms highlighted.
//!
//! [`SnippetGenerator`] serves this purpose.
//! It scans a document and constructs a snippet, which consists of sections where the search terms
//! have been found, stitched together with "..." in between sections if necessary.
//!
//! ## Example: Single Snippet
//!
//! ```rust
//! # use tantivy::query::QueryParser;
//! # use tantivy::schema::{Schema, TEXT};
//! # use tantivy::{doc, Index};
//! use tantivy::snippet::SnippetGenerator;
//!
//! # fn main() -> tantivy::Result<()> {
//! #    let mut schema_builder = Schema::builder();
//! #    let text_field = schema_builder.add_text_field("text", TEXT);
//! #    let schema = schema_builder.build();
//! #    let index = Index::create_in_ram(schema);
//! #    let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
//! #    let doc = doc!(text_field => r#"Comme je descendais des Fleuves impassibles,
//! #   Je ne me sentis plus guidé par les haleurs :
//! #  Des Peaux-Rouges criards les avaient pris pour cibles,
//! #  Les ayant cloués nus aux poteaux de couleurs.
//! #
//! #  J'étais insoucieux de tous les équipages,
//! #  Porteur de blés flamands ou de cotons anglais.
//! #  Quand avec mes haleurs ont fini ces tapages,
//! #  Les Fleuves m'ont laissé descendre où je voulais.
//! #  "#);
//! #    index_writer.add_document(doc.clone())?;
//! #    index_writer.commit()?;
//! #    let query_parser = QueryParser::for_index(&index, vec![text_field]);
//! // ...
//! let query = query_parser.parse_query("haleurs flamands").unwrap();
//! # let reader = index.reader()?;
//! # let searcher = reader.searcher();
//! let mut snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
//! snippet_generator.set_max_num_chars(100);
//! let snippet = snippet_generator.snippet_from_doc(&doc);
//! let snippet_html: String = snippet.to_html();
//! assert_eq!(snippet_html, "Comme je descendais des Fleuves impassibles,\n  Je ne me sentis plus guidé par les <b>haleurs</b> :\n Des");
//! #    Ok(())
//! # }
//! ```
//!
//! ## Example: Multiple Snippets
//!
//! For long documents with matches in different locations, you can retrieve multiple snippets.
//! By default, snippets are sorted by score. You can change this to sort by position.
//!
//! ```rust
//! # use tantivy::query::QueryParser;
//! # use tantivy::schema::{Schema, TEXT};
//! # use tantivy::{doc, Index};
//! use tantivy::snippet::{SnippetGenerator, SnippetSortOrder};
//!
//! # fn main() -> tantivy::Result<()> {
//! #    let mut schema_builder = Schema::builder();
//! #    let text_field = schema_builder.add_text_field("text", TEXT);
//! #    let schema = schema_builder.build();
//! #    let index = Index::create_in_ram(schema);
//! #    let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
//! #    let doc = doc!(text_field => "Rust is at the beginning. Some filler text here. Rust appears in the middle. More text. Rust is at the end.");
//! #    index_writer.add_document(doc.clone())?;
//! #    index_writer.commit()?;
//! #    let query_parser = QueryParser::for_index(&index, vec![text_field]);
//! let query = query_parser.parse_query("rust").unwrap();
//! # let reader = index.reader()?;
//! # let searcher = reader.searcher();
//! let mut snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
//! snippet_generator.set_max_num_chars(50);
//! snippet_generator.set_snippets_limit(3); // Get up to 3 snippets
//! snippet_generator.set_sort_order(SnippetSortOrder::Position); // Sort by position
//!
//! let snippets = snippet_generator.snippets_from_doc(&doc);
//! // Returns multiple snippets, one for each match location
//! assert!(snippets.len() >= 2); // At least 2 snippets for different match locations
//! for snippet in &snippets {
//!     assert!(snippet.to_html().to_lowercase().contains("rust"));
//! }
//! #    Ok(())
//! # }
//! ```
//!
//! You can also specify the maximum number of characters for the snippets generated with the
//! `set_max_num_chars` method. By default, this limit is set to 150.
//!
//! To get multiple snippets instead of just one, use `set_snippets_limit()` to specify
//! how many snippets to return (default is 1). Set to 0 to return all matching snippets.
//!
//! SnippetGenerator needs to be created from the `Searcher` and the query, and the field on which
//! the `SnippetGenerator` should generate the snippets.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use htmlescape::encode_minimal;

use crate::query::Query;
use crate::schema::document::{Document, Value};
use crate::schema::Field;
use crate::tokenizer::{TextAnalyzer, Token};
use crate::{Score, Searcher, Term};

/// The sort order for snippets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SnippetSortOrder {
    /// Sort by relevance score (descending). Snippets with the same score are sorted by position.
    #[default]
    Score,
    /// Sort by position in the document (ascending).
    Position,
}

const DEFAULT_MAX_NUM_CHARS: usize = 150;

const DEFAULT_SNIPPET_PREFIX: &str = "<b>";
const DEFAULT_SNIPPET_POSTFIX: &str = "</b>";

#[derive(Debug)]
pub(crate) struct FragmentCandidate {
    start_offset: usize,
    stop_offset: usize,
    highlighted: Vec<(Range<usize>, Score)>,
}

impl FragmentCandidate {
    /// Create a basic `FragmentCandidate`
    ///
    /// `score`, `num_chars` are set to 0
    /// and `highlighted` is set to empty vec
    /// stop_offset is set to start_offset, which is taken as a param.
    fn new(start_offset: usize) -> FragmentCandidate {
        FragmentCandidate {
            start_offset,
            stop_offset: start_offset,
            highlighted: vec![],
        }
    }

    /// Updates `score` and `highlighted` fields of the objects.
    ///
    /// taking the token and terms, the token is added to the fragment.
    /// if the token is one of the terms, the score
    /// and highlighted fields are updated in the fragment.
    fn try_add_token(&mut self, token: &Token, terms: &BTreeMap<String, Score>) {
        self.stop_offset = token.offset_to;

        if let Some(&score) = terms.get(&token.text.to_lowercase()) {
            self.highlighted
                .push((token.offset_from..token.offset_to, score));
        }
    }

    fn score(&self) -> Score {
        self.highlighted.iter().map(|(_, score)| score).sum()
    }

    fn len(&self) -> usize {
        self.highlighted.len()
    }

    fn is_empty(&self) -> bool {
        self.highlighted.is_empty()
    }
}

/// `Snippet`
/// Contains a fragment of a document, and some highlighted parts inside it.
#[derive(Debug)]
pub struct Snippet {
    fragment: String,
    highlighted: Vec<Range<usize>>,
    snippet_prefix: String,
    snippet_postfix: String,
}

impl Snippet {
    /// Create a new `Snippet`.
    fn new(fragment: &str, highlighted: Vec<Range<usize>>) -> Self {
        Self {
            fragment: fragment.to_string(),
            highlighted,
            snippet_prefix: DEFAULT_SNIPPET_PREFIX.to_string(),
            snippet_postfix: DEFAULT_SNIPPET_POSTFIX.to_string(),
        }
    }

    /// Create a new, empty, `Snippet`.
    pub fn empty() -> Snippet {
        Snippet {
            fragment: String::new(),
            highlighted: Vec::new(),
            snippet_prefix: String::new(),
            snippet_postfix: String::new(),
        }
    }

    /// Returns `true` if the snippet is empty.
    pub fn is_empty(&self) -> bool {
        self.highlighted.is_empty()
    }

    /// Returns a highlighted html from the `Snippet`.
    pub fn to_html(&self) -> String {
        let mut html = String::new();
        let mut start_from: usize = 0;

        for item in collapse_overlapped_ranges(&self.highlighted) {
            html.push_str(&encode_minimal(&self.fragment[start_from..item.start]));
            html.push_str(&self.snippet_prefix);
            html.push_str(&encode_minimal(&self.fragment[item.clone()]));
            html.push_str(&self.snippet_postfix);
            start_from = item.end;
        }
        html.push_str(&encode_minimal(
            &self.fragment[start_from..self.fragment.len()],
        ));
        html
    }

    /// Returns the fragment of text used in the  snippet.
    pub fn fragment(&self) -> &str {
        &self.fragment
    }

    /// Returns a list of highlighted positions from the `Snippet`.
    pub fn highlighted(&self) -> &[Range<usize>] {
        &self.highlighted
    }

    /// Sets highlighted prefix and postfix.
    pub fn set_snippet_prefix_postfix(&mut self, prefix: &str, postfix: &str) {
        self.snippet_prefix = prefix.to_string();
        self.snippet_postfix = postfix.to_string()
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
fn search_fragments(
    tokenizer: &mut TextAnalyzer,
    text: &str,
    terms: &BTreeMap<String, Score>,
    max_num_chars: usize,
    matches_limit: Option<usize>,
    matches_offset: Option<usize>,
) -> Vec<FragmentCandidate> {
    let mut token_stream = tokenizer.token_stream(text);
    let mut fragment = FragmentCandidate::new(0);
    let mut fragments: Vec<FragmentCandidate> = vec![];

    // Process all fragments first, without applying offset/limit to token stream
    while let Some(next) = token_stream.next() {
        if (next.offset_to - fragment.start_offset) > max_num_chars {
            if fragment.score() > 0.0 {
                fragments.push(fragment)
            };
            fragment = FragmentCandidate::new(next.offset_from);
        }

        fragment.try_add_token(next, terms);
    }
    if fragment.score() > 0.0 {
        fragments.push(fragment)
    }

    if matches_offset.is_none() && matches_limit.is_none() {
        return fragments;
    }

    // Skip the first offset snippets, and take the next limit snippets
    // across all FragmentCandidates
    let offset_count = matches_offset.unwrap_or(0);
    let limit_count = matches_limit.unwrap_or(fragments.len());

    let mut remaining_offset = offset_count;
    let mut remaining_limit = limit_count;
    let mut filtered_fragments = Vec::new();

    for mut fragment in fragments {
        if remaining_limit == 0 {
            break;
        }

        let num_snippets = fragment.len();

        if remaining_offset >= num_snippets {
            remaining_offset -= num_snippets;
            continue;
        }

        let skip_from_this_fragment = remaining_offset;
        let take_from_this_fragment =
            std::cmp::min(num_snippets - skip_from_this_fragment, remaining_limit);

        fragment.highlighted = fragment
            .highlighted
            .into_iter()
            .skip(skip_from_this_fragment)
            .take(take_from_this_fragment)
            .collect();

        remaining_offset = 0;
        remaining_limit -= take_from_this_fragment;

        if !fragment.is_empty() {
            filtered_fragments.push(fragment);
        }
    }

    filtered_fragments
}

/// Returns a Snippet
///
/// Takes a vector of `FragmentCandidate`s and the text.
/// Figures out the best fragment from it and creates a snippet.
fn select_best_fragment_combination(fragments: &[FragmentCandidate], text: &str) -> Snippet {
    let best_fragment_opt = fragments.iter().max_by(|left, right| {
        let cmp_score = left
            .score()
            .partial_cmp(&right.score())
            .unwrap_or(Ordering::Equal);
        if cmp_score == Ordering::Equal {
            (right.start_offset, right.stop_offset).cmp(&(left.start_offset, left.stop_offset))
        } else {
            cmp_score
        }
    });
    if let Some(fragment) = best_fragment_opt {
        let fragment_text = &text[fragment.start_offset..fragment.stop_offset];
        let highlighted = fragment
            .highlighted
            .iter()
            .map(|(item, _)| item.start - fragment.start_offset..item.end - fragment.start_offset)
            .collect();
        Snippet::new(fragment_text, highlighted)
    } else {
        // When there are no fragments to chose from,
        // for now create an empty snippet.
        Snippet::empty()
    }
}

/// Returns multiple Snippets
///
/// Takes a vector of `FragmentCandidate`s, the text, and the number of snippets to return.
/// Selects snippets based on the `sort_order` and returns them in that order, after pagination.
fn select_top_fragments(
    fragments: &[FragmentCandidate],
    text: &str,
    snippets_limit: usize,
    snippets_offset: usize,
    sort_order: SnippetSortOrder,
) -> Vec<Snippet> {
    if fragments.is_empty() {
        return vec![];
    }

    let mut all_fragments: Vec<&FragmentCandidate> = fragments.iter().collect();
    // If the offset and limit select less than the entire result set, we use `select_nth` to
    // prune the suffix before sorting.
    let relevant_prefix_count =
        if snippets_limit > 0 && snippets_limit + snippets_offset < all_fragments.len() {
            Some(snippets_limit + snippets_offset)
        } else {
            None
        };
    let mut relevant_fragments = &mut all_fragments[..];

    // Sort by the specified order to apply pagination consistently.
    match sort_order {
        SnippetSortOrder::Score => {
            let sort_by = |a: &&FragmentCandidate, b: &&FragmentCandidate| {
                b.score()
                    .partial_cmp(&a.score())
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| a.start_offset.cmp(&b.start_offset))
            };
            if let Some(prefix_count) = relevant_prefix_count {
                let (prefix, _, _) =
                    relevant_fragments.select_nth_unstable_by(prefix_count, sort_by);
                relevant_fragments = prefix;
            }
            relevant_fragments.sort_unstable_by(sort_by);
        }
        SnippetSortOrder::Position => {
            let sort_by_key = |fragment: &&FragmentCandidate| fragment.start_offset;
            if let Some(prefix_count) = relevant_prefix_count {
                let (prefix, _, _) =
                    relevant_fragments.select_nth_unstable_by_key(prefix_count, sort_by_key);
                relevant_fragments = prefix;
            }
            relevant_fragments.sort_unstable_by_key(sort_by_key);
        }
    }

    // Apply offset the offset.
    let start = snippets_offset.min(relevant_fragments.len());
    let selected = &relevant_fragments[start..];

    // Convert to Snippets
    selected
        .iter()
        .map(|&fragment| {
            let fragment_text = &text[fragment.start_offset..fragment.stop_offset];
            let highlighted = fragment
                .highlighted
                .iter()
                .map(|(item, _)| {
                    item.start - fragment.start_offset..item.end - fragment.start_offset
                })
                .collect();
            Snippet::new(fragment_text, highlighted)
        })
        .collect()
}

/// Sorts and removes duplicate ranges from the input.
///
/// This function first sorts the ranges by their start position,
/// then by their end position, and finally removes any duplicate ranges.
///
/// ## Examples
/// - [0..3, 3..6, 0..3, 3..6] -> [0..3, 3..6]
/// - [2..4, 1..3, 2..4, 0..2] -> [0..2, 1..3, 2..4]
fn sort_and_deduplicate_ranges(ranges: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut sorted_ranges = ranges.to_vec();
    sorted_ranges.sort_by_key(|range| (range.start, range.end));
    sorted_ranges.dedup();
    sorted_ranges
}

/// Merges overlapping or adjacent ranges into non-overlapping ranges.
///
/// This function assumes that the input ranges are already sorted
/// and deduplicated. Use `sort_and_deduplicate_ranges` before calling
/// this function if the input might contain unsorted or duplicate ranges.
///
/// ## Examples
/// - [0..1, 2..3] -> [0..1, 2..3]  # no overlap
/// - [0..1, 1..2] -> [0..2]  # adjacent, merged
/// - [0..2, 1..3] -> [0..3]  # overlapping, merged
/// - [0..3, 1..2] -> [0..3]  # second range is completely within the first
fn merge_overlapping_ranges(ranges: &[Range<usize>]) -> Vec<Range<usize>> {
    debug_assert!(is_sorted(ranges.iter().map(|range| range.start)));
    let mut result = Vec::<Range<usize>>::new();
    for range in ranges {
        if let Some(last) = result.last_mut() {
            if last.end > range.start {
                // Only merge when there is a true overlap.
                last.end = std::cmp::max(last.end, range.end);
            } else {
                // Do not overlap or only adjacent, add new scope.
                result.push(range.clone());
            }
        } else {
            // The first range
            result.push(range.clone());
        }
    }
    result
}

/// Collapses ranges into non-overlapped ranges.
///
/// This function first sorts and deduplicates the input ranges,
/// then merges any overlapping or adjacent ranges.
///
/// ## Examples
/// - [0..1, 2..3] -> [0..1, 2..3]  # no overlap
/// - [0..1, 1..2] -> [0..2]  # adjacent, merged
/// - [0..2, 1..3] -> [0..3]  # overlapping, merged
/// - [0..3, 1..2] -> [0..3]  # second range is completely within the first
/// - [0..3, 3..6, 0..3, 3..6] -> [0..6]  # duplicates removed, then merged
pub fn collapse_overlapped_ranges(ranges: &[Range<usize>]) -> Vec<Range<usize>> {
    let prepared = sort_and_deduplicate_ranges(ranges);
    merge_overlapping_ranges(&prepared)
}

fn is_sorted(mut it: impl Iterator<Item = usize>) -> bool {
    if let Some(first) = it.next() {
        let mut prev = first;
        for item in it {
            if item < prev {
                return false;
            }
            prev = item;
        }
    }
    true
}

/// `SnippetGenerator`
///
/// # Example
///
/// ```rust
/// # use tantivy::query::QueryParser;
/// # use tantivy::schema::{Schema, TEXT};
/// # use tantivy::{doc, Index};
/// use tantivy::snippet::SnippetGenerator;
///
/// # fn main() -> tantivy::Result<()> {
/// #    let mut schema_builder = Schema::builder();
/// #    let text_field = schema_builder.add_text_field("text", TEXT);
/// #    let schema = schema_builder.build();
/// #    let index = Index::create_in_ram(schema);
/// #    let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
/// #    let doc = doc!(text_field => r#"Comme je descendais des Fleuves impassibles,
/// #   Je ne me sentis plus guidé par les haleurs :
/// #  Des Peaux-Rouges criards les avaient pris pour cibles,
/// #  Les ayant cloués nus aux poteaux de couleurs.
/// #
/// #  J'étais insoucieux de tous les équipages,
/// #  Porteur de blés flamands ou de cotons anglais.
/// #  Quand avec mes haleurs ont fini ces tapages,
/// #  Les Fleuves m'ont laissé descendre où je voulais.
/// #  "#);
/// #    index_writer.add_document(doc.clone())?;
/// #    index_writer.commit()?;
/// #    let query_parser = QueryParser::for_index(&index, vec![text_field]);
/// // ...
/// let query = query_parser.parse_query("haleurs flamands").unwrap();
/// # let reader = index.reader()?;
/// # let searcher = reader.searcher();
/// let mut snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
/// snippet_generator.set_max_num_chars(100);
/// let snippet = snippet_generator.snippet_from_doc(&doc);
/// let snippet_html: String = snippet.to_html();
/// assert_eq!(snippet_html, "Comme je descendais des Fleuves impassibles,\n  Je ne me sentis plus guidé par les <b>haleurs</b> :\n Des");
/// #    Ok(())
/// # }
/// ```
pub struct SnippetGenerator {
    terms_text: BTreeMap<String, Score>,
    tokenizer: TextAnalyzer,
    field: Field,
    max_num_chars: usize,
    matches_limit: Option<usize>,
    matches_offset: Option<usize>,
    snippets_limit: usize,
    snippets_offset: usize,
    sort_order: SnippetSortOrder,
}

impl SnippetGenerator {
    /// Creates a new snippet generator
    pub fn new(
        terms_text: BTreeMap<String, Score>,
        tokenizer: TextAnalyzer,
        field: Field,
        max_num_chars: usize,
    ) -> Self {
        SnippetGenerator {
            terms_text,
            tokenizer,
            field,
            max_num_chars,
            matches_limit: None,
            matches_offset: None,
            snippets_limit: 1,
            snippets_offset: 0,
            sort_order: SnippetSortOrder::default(),
        }
    }

    pub fn set_matches_limit(&mut self, limit: usize) {
        self.matches_limit = Some(limit);
    }

    pub fn set_matches_offset(&mut self, offset: usize) {
        self.matches_offset = Some(offset);
    }

    /// Creates a new snippet generator
    pub fn create(
        searcher: &Searcher,
        query: &dyn Query,
        field: Field,
    ) -> crate::Result<SnippetGenerator> {
        let mut terms: BTreeSet<Term> = BTreeSet::new();
        for segment_reader in searcher.segment_readers() {
            query.query_terms(field, segment_reader, &mut |term, _| {
                if term.field() == field {
                    terms.insert(term.clone());
                }
            });
        }
        let mut terms_text: BTreeMap<String, Score> = Default::default();
        for term in terms {
            let term_value = term.value();
            let term_str = if let Some(term_str) = term_value.as_str() {
                term_str
            } else if let Some(json_value_bytes) =
                term_value.as_json_value_bytes().map(|v| v.to_owned())
            {
                if let Some(json_str) = json_value_bytes.as_str() {
                    &json_str.to_string()
                } else {
                    continue;
                }
            } else {
                continue;
            };
            let doc_freq = searcher.doc_freq(&term)?;
            if doc_freq > 0 {
                let score = 1.0 / (1.0 + doc_freq as Score);
                terms_text.insert(term_str.to_string(), score);
            }
        }
        let tokenizer = searcher.index().tokenizer_for_field(field)?;
        Ok(SnippetGenerator {
            terms_text,
            tokenizer,
            field,
            max_num_chars: DEFAULT_MAX_NUM_CHARS,
            matches_limit: None,
            matches_offset: None,
            snippets_limit: 1,
            snippets_offset: 0,
            sort_order: SnippetSortOrder::default(),
        })
    }

    /// Sets a maximum number of chars. Default is 150.
    pub fn set_max_num_chars(&mut self, max_num_chars: usize) {
        self.max_num_chars = max_num_chars;
    }

    /// Sets the number of snippets to return. Default is 1.
    /// Set to 0 to return all snippets (unlimited).
    pub fn set_snippets_limit(&mut self, snippets_limit: usize) {
        self.snippets_limit = snippets_limit;
    }

    /// Sets the offset for the snippets to return. Default is 0.
    pub fn set_snippets_offset(&mut self, snippets_offset: usize) {
        self.snippets_offset = snippets_offset;
    }

    /// Sets the sort order for snippets. Default is by score.
    pub fn set_sort_order(&mut self, sort_order: SnippetSortOrder) {
        self.sort_order = sort_order;
    }

    #[cfg(test)]
    pub(crate) fn terms_text(&self) -> &BTreeMap<String, Score> {
        &self.terms_text
    }

    /// Generates a snippet for the given `Document`.
    ///
    /// This method extract the text associated with the `SnippetGenerator`'s field
    /// and computes a snippet.
    pub fn snippet_from_doc<D: Document>(&self, doc: &D) -> Snippet {
        let mut text = String::new();
        for (field, value) in doc.iter_fields_and_values() {
            let value = value as D::Value<'_>;
            if field != self.field {
                continue;
            }

            if let Some(val) = value.as_str() {
                text.push(' ');
                text.push_str(val);
            }
        }

        self.snippet(text.trim())
    }

    /// Generates a snippet for the given text.
    pub fn snippet(&self, text: &str) -> Snippet {
        let fragment_candidates = search_fragments(
            &mut self.tokenizer.clone(),
            text,
            &self.terms_text,
            self.max_num_chars,
            self.matches_limit,
            self.matches_offset,
        );
        select_best_fragment_combination(&fragment_candidates[..], text)
    }

    /// Generates multiple snippets for the given text.
    ///
    /// Returns up to `snippets_limit` snippets, sorted according to the `sort_order` setting.
    /// The `snippets_offset` and `snippets_limit` parameters are applied to this sorted
    /// list of snippets, allowing for consistent paging through the results.
    ///
    /// If `snippets_limit` is set to 0 (via `set_snippets_limit`), all matching snippets
    /// are returned.
    pub fn snippets(&self, text: &str) -> Vec<Snippet> {
        let fragment_candidates = search_fragments(
            &mut self.tokenizer.clone(),
            text,
            &self.terms_text,
            self.max_num_chars,
            self.matches_limit,
            self.matches_offset,
        );
        select_top_fragments(
            &fragment_candidates[..],
            text,
            self.snippets_limit,
            self.snippets_offset,
            self.sort_order,
        )
    }

    /// Generates multiple snippets for the given `Document`.
    ///
    /// This method extracts the text associated with the `SnippetGenerator`'s field
    /// and computes multiple snippets.
    ///
    /// Returns up to `snippets_limit` snippets, sorted according to the `sort_order` setting.
    /// The `snippets_offset` and `snippets_limit` parameters are applied to this sorted
    /// list of snippets, allowing for consistent paging through the results.
    pub fn snippets_from_doc<D: Document>(&self, doc: &D) -> Vec<Snippet> {
        let mut text = String::new();
        for (field, value) in doc.iter_fields_and_values() {
            let value = value as D::Value<'_>;
            if field != self.field {
                continue;
            }

            if let Some(val) = value.as_str() {
                text.push(' ');
                text.push_str(val);
            }
        }

        self.snippets(text.trim())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Range;

    use maplit::btreemap;

    use super::{
        collapse_overlapped_ranges, search_fragments, select_best_fragment_combination,
        select_top_fragments,
    };
    use crate::query::QueryParser;
    use crate::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions, TEXT};
    use crate::snippet::{SnippetGenerator, SnippetSortOrder};
    use crate::tokenizer::{NgramTokenizer, SimpleTokenizer};
    use crate::Index;

    const TEST_TEXT: &str = r#"Rust is a systems programming language sponsored by
Mozilla which describes it as a "safe, concurrent, practical language", supporting functional and
imperative-procedural paradigms. Rust is syntactically similar to C++[according to whom?],
but its designers intend it to provide better memory safety while still maintaining
performance.

Rust is free and open-source software, released under an MIT License, or Apache License
2.0. Its designers have refined the language through the experiences of writing the Servo
web browser layout engine[14] and the Rust compiler. A large proportion of current commits
to the project are from community members.[15]

Rust won first place for "most loved programming language" in the Stack Overflow Developer
Survey in 2016, 2017, and 2018."#;

    #[test]
    fn test_snippet() {
        let terms = btreemap! {
            String::from("rust") => 1.0,
            String::from("language") => 0.9
        };
        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            TEST_TEXT,
            &terms,
            100,
            None,
            None,
        );
        assert_eq!(fragments.len(), 7);
        {
            let first = &fragments[0];
            assert_eq!(first.score(), 1.9);
            assert_eq!(first.stop_offset, 89);
        }
        let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
        assert_eq!(
            snippet.fragment,
            "Rust is a systems programming language sponsored by\nMozilla which describes it as a \
             \"safe"
        );
        assert_eq!(
            snippet.to_html(),
            "<b>Rust</b> is a systems programming <b>language</b> sponsored by\nMozilla which \
             describes it as a &quot;safe"
        )
    }

    #[test]
    fn test_snippet_scored_fragment() {
        {
            let terms = btreemap! {
                String::from("rust") =>1.0,
                String::from("language") => 0.9
            };
            let fragments = search_fragments(
                &mut From::from(SimpleTokenizer::default()),
                TEST_TEXT,
                &terms,
                20,
                None,
                None,
            );
            {
                let first = &fragments[0];
                assert_eq!(first.score(), 1.0);
                assert_eq!(first.stop_offset, 17);
            }
            let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
            assert_eq!(snippet.to_html(), "<b>Rust</b> is a systems")
        }
        {
            let terms = btreemap! {
                String::from("rust") =>0.9,
                String::from("language") => 1.0
            };
            let fragments = search_fragments(
                &mut From::from(SimpleTokenizer::default()),
                TEST_TEXT,
                &terms,
                20,
                None,
                None,
            );
            // assert_eq!(fragments.len(), 7);
            {
                let first = &fragments[0];
                assert_eq!(first.score(), 0.9);
                assert_eq!(first.stop_offset, 17);
            }
            let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
            assert_eq!(snippet.to_html(), "programming <b>language</b>")
        }
    }

    #[test]
    fn test_snippet_in_second_fragment() {
        let text = "a b c d e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("c"), 1.0);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            3,
            None,
            None,
        );

        assert_eq!(fragments.len(), 1);
        {
            let first = &fragments[0];
            assert_eq!(first.score(), 1.0);
            assert_eq!(first.start_offset, 4);
            assert_eq!(first.stop_offset, 7);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "c d");
        assert_eq!(snippet.to_html(), "<b>c</b> d");
    }

    #[test]
    fn test_snippet_with_term_at_the_end_of_fragment() {
        let text = "a b c d e f f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("f"), 1.0);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            3,
            None,
            None,
        );

        assert_eq!(fragments.len(), 2);
        {
            let first = &fragments[0];
            assert_eq!(first.score(), 1.0);
            assert_eq!(first.stop_offset, 11);
            assert_eq!(first.start_offset, 8);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "e f");
        assert_eq!(snippet.to_html(), "e <b>f</b>");
    }

    #[test]
    fn test_snippet_with_second_fragment_has_the_highest_score() {
        let text = "a b c d e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("f"), 1.0);
        terms.insert(String::from("a"), 0.9);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            7,
            None,
            None,
        );

        assert_eq!(fragments.len(), 2);
        {
            let first = &fragments[0];
            assert_eq!(first.score(), 0.9);
            assert_eq!(first.stop_offset, 7);
            assert_eq!(first.start_offset, 0);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "e f g");
        assert_eq!(snippet.to_html(), "e <b>f</b> g");
    }

    #[test]
    fn test_snippet_with_term_not_in_text() {
        let text = "a b c d";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("z"), 1.0);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            3,
            None,
            None,
        );

        assert_eq!(fragments.len(), 0);

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "");
        assert_eq!(snippet.to_html(), "");
        assert!(snippet.is_empty());
    }

    #[test]
    fn test_snippet_with_no_terms() {
        let text = "a b c d";

        let terms = BTreeMap::new();
        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            3,
            None,
            None,
        );
        assert_eq!(fragments.len(), 0);

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "");
        assert_eq!(snippet.to_html(), "");
        assert!(snippet.is_empty());
    }

    #[test]
    fn test_snippet_generator_term_score() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field => "a"))?;
            index_writer.add_document(doc!(text_field => "a"))?;
            index_writer.add_document(doc!(text_field => "a b"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        {
            let query = query_parser.parse_query("e")?;
            let snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
            assert!(snippet_generator.terms_text().is_empty());
        }
        {
            let query = query_parser.parse_query("a")?;
            let snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
            assert_eq!(
                &btreemap!("a".to_string() => 0.25),
                snippet_generator.terms_text()
            );
        }
        {
            let query = query_parser.parse_query("a b")?;
            let snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
            assert_eq!(
                &btreemap!("a".to_string() => 0.25, "b".to_string() => 0.5),
                snippet_generator.terms_text()
            );
        }
        {
            let query = query_parser.parse_query("a b c")?;
            let snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field)?;
            assert_eq!(
                &btreemap!("a".to_string() => 0.25, "b".to_string() => 0.5),
                snippet_generator.terms_text()
            );
        }
        Ok(())
    }

    #[test]
    fn test_snippet_generator() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("en_stem")
                .set_index_option(IndexRecordOption::Basic),
        );
        let text_field = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_for_tests()?;
            let doc = doc!(text_field => TEST_TEXT);
            index_writer.add_document(doc)?;
            index_writer.commit()?;
        }
        let searcher = index.reader().unwrap().searcher();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("rust design").unwrap();
        let mut snippet_generator =
            SnippetGenerator::create(&searcher, &*query, text_field).unwrap();
        {
            let snippet = snippet_generator.snippet(TEST_TEXT);
            assert_eq!(
                snippet.to_html(),
                "imperative-procedural paradigms. <b>Rust</b> is syntactically similar to \
                 C++[according to whom?],\nbut its <b>designers</b> intend it to provide better \
                 memory safety"
            );
        }
        {
            snippet_generator.set_max_num_chars(90);
            let snippet = snippet_generator.snippet(TEST_TEXT);
            assert_eq!(
                snippet.to_html(),
                "<b>Rust</b> is syntactically similar to C++[according to whom?],\nbut its \
                 <b>designers</b> intend it to"
            );
        }
        Ok(())
    }

    #[test]
    fn test_snippet_with_overlapped_highlighted_ranges() {
        let text = "abc";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("ab"), 0.9);
        terms.insert(String::from("bc"), 1.0);

        let fragments = search_fragments(
            &mut From::from(NgramTokenizer::all_ngrams(2, 2).unwrap()),
            text,
            &terms,
            3,
            None,
            None,
        );

        assert_eq!(fragments.len(), 1);
        {
            let first = &fragments[0];
            assert_eq!(first.score(), 1.9);
            assert_eq!(first.start_offset, 0);
            assert_eq!(first.stop_offset, 3);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "abc");
        assert_eq!(snippet.to_html(), "<b>abc</b>");
    }

    #[test]
    fn test_snippet_generator_custom_highlighted_elements() {
        let terms = btreemap! { String::from("rust") => 1.0, String::from("language") => 0.9 };
        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            TEST_TEXT,
            &terms,
            100,
            None,
            None,
        );
        let mut snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
        assert_eq!(
            snippet.to_html(),
            "<b>Rust</b> is a systems programming <b>language</b> sponsored by\nMozilla which \
             describes it as a &quot;safe"
        );
        snippet.set_snippet_prefix_postfix("<q class=\"super\">", "</q>");
        assert_eq!(
            snippet.to_html(),
            "<q class=\"super\">Rust</q> is a systems programming <q class=\"super\">language</q> \
             sponsored by\nMozilla which describes it as a &quot;safe"
        );
    }

    #[test]
    fn test_snippet_with_limit_and_offset() {
        let terms = btreemap! {
            String::from("rust") => 1.0,
            String::from("language") => 0.9,
        };
        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            TEST_TEXT,
            &terms,
            100,
            Some(2), // matches_limit
            Some(1), // matches_offset
        );
        assert_eq!(fragments.len(), 2);
        {
            let first = &fragments[0];
            assert_eq!(first.score(), 0.9);
            assert_eq!(first.stop_offset, 89);
        }
        {
            let second = &fragments[1];
            assert_eq!(second.score(), 0.9);
            assert_eq!(second.stop_offset, 190);
        }
        let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
        assert_eq!(
            snippet.fragment,
            "Rust is a systems programming language sponsored by\nMozilla which describes it as a \
             \"safe"
        );
        assert_eq!(
            snippet.to_html(),
            "Rust is a systems programming <b>language</b> sponsored by\nMozilla which describes \
             it as a &quot;safe"
        )
    }

    #[test]
    fn test_collapse_overlapped_ranges() {
        #![allow(clippy::single_range_in_vec_init)]
        assert_eq!(&collapse_overlapped_ranges(&[0..1, 2..3]), &[0..1, 2..3]);
        assert_eq!(&collapse_overlapped_ranges(&[0..1, 1..2]), &[0..1, 1..2]);
        assert_eq!(&collapse_overlapped_ranges(&[0..2, 1..2]), &[0..2]);
        assert_eq!(&collapse_overlapped_ranges(&[0..2, 1..3]), &[0..3]);
        assert_eq!(&collapse_overlapped_ranges(&[0..3, 1..2]), &[0..3]);
    }

    #[test]
    fn test_no_overlap() {
        let ranges = vec![0..1, 2..3, 4..5];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..1, 2..3, 4..5]);
    }

    #[test]
    fn test_adjacent_ranges() {
        let ranges = vec![0..1, 1..2, 2..3];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..1, 1..2, 2..3]);
    }

    #[test]
    fn test_overlapping_ranges() {
        let ranges = vec![0..2, 1..3, 2..4];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..4]);
    }

    #[test]
    fn test_contained_ranges() {
        let ranges = vec![0..5, 1..2, 3..4];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..5]);
    }

    #[test]
    fn test_duplicate_ranges() {
        let ranges = vec![0..2, 2..4, 0..2, 2..4];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..2, 2..4]);
    }

    #[test]
    fn test_unsorted_ranges() {
        let ranges = vec![2..4, 0..2, 1..3];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..4]);
    }

    #[test]
    fn test_complex_scenario() {
        let ranges = vec![0..2, 5..7, 1..3, 8..9, 2..4, 3..6, 8..10];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..7, 8..10]);
    }

    #[test]
    fn test_empty_input() {
        let ranges: Vec<Range<usize>> = vec![];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, ranges);
    }

    #[test]
    fn test_single_range() {
        #![allow(clippy::single_range_in_vec_init)]
        let ranges = vec![0..5];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..5]);
    }

    #[test]
    fn test_zero_length_ranges() {
        let ranges = vec![0..0, 1..1, 2..2, 3..3];
        let result = collapse_overlapped_ranges(&ranges);
        assert_eq!(result, vec![0..0, 1..1, 2..2, 3..3]);
    }

    #[test]
    fn test_snippets_multiple_fragments() {
        let text = "a rust b c d language e f g rust h i j language k l m";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);
        terms.insert(String::from("language"), 0.9);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            10,
            None,
            None,
        );

        // Should have 4 fragments, each containing one match
        assert_eq!(fragments.len(), 4);

        // Test getting top 2 snippets
        let snippets = select_top_fragments(&fragments[..], text, 2, 0, SnippetSortOrder::Position);
        assert_eq!(snippets.len(), 2);

        // The first snippet should contain "rust"
        assert!(snippets[0].to_html().contains("<b>rust</b>"));
        // The second snippet should contain "language"
        assert!(snippets[1].to_html().contains("<b>language</b>"));
    }

    #[test]
    fn test_snippets_unlimited_fragments() {
        let text = "a rust b c d language e f g rust h i j language k l m";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);
        terms.insert(String::from("language"), 0.9);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            10,
            None,
            None,
        );

        // Test getting all snippets (num_snippets = 0)
        let snippets = select_top_fragments(&fragments[..], text, 0, 0, SnippetSortOrder::Position);
        assert_eq!(snippets.len(), 4);

        // Verify they are in document order
        assert!(snippets[0].fragment().contains("rust"));
        assert!(snippets[1].fragment().contains("language"));
        assert!(snippets[2].fragment().contains("rust"));
        assert!(snippets[3].fragment().contains("language"));
    }

    #[test]
    fn test_snippets_more_requested_than_available() {
        let text = "a rust b c d language e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);
        terms.insert(String::from("language"), 0.9);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            15,
            None,
            None,
        );

        // Only 2 fragments available
        assert_eq!(fragments.len(), 2);

        // Request 10 snippets, should only get 2
        let snippets =
            select_top_fragments(&fragments[..], text, 10, 0, SnippetSortOrder::Position);
        assert_eq!(snippets.len(), 2);
    }

    #[test]
    fn test_snippets_score_based_selection() {
        let text = "a b c rust d e f rust rust g h i language j k l";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);
        terms.insert(String::from("language"), 0.9);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            15,
            None,
            None,
        );

        // Request top 2 snippets
        let snippets = select_top_fragments(&fragments[..], text, 2, 0, SnippetSortOrder::Score);
        assert_eq!(snippets.len(), 2);

        // The fragment with "rust rust" should have highest score (2.0)
        // and should be first in results (after re-sorting by position)
        let has_double_rust = snippets
            .iter()
            .any(|s| s.fragment().matches("rust").count() == 2);
        assert!(has_double_rust);
    }

    #[test]
    fn test_snippets_position_tiebreaker() {
        let text = "a rust b c d e f rust g h i rust j k l";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            10,
            None,
            None,
        );

        // All snippets have same score (1.0), so position should be tie-breaker
        let snippets = select_top_fragments(&fragments[..], text, 2, 0, SnippetSortOrder::Position);
        assert_eq!(snippets.len(), 2);

        // Should get first two occurrences
        assert_eq!(snippets[0].fragment(), "a rust b c");
        assert_eq!(snippets[1].fragment(), "d e f rust");
    }

    #[test]
    fn test_snippets_with_offset() {
        let text = "a rust b c d rust e f g rust h i j";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("rust"), 1.0);

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            10,
            None,
            None,
        );

        // All snippets have same score (1.0)
        // Request 2 snippets, with offset 1
        let snippets = select_top_fragments(&fragments[..], text, 2, 1, SnippetSortOrder::Position);
        assert_eq!(snippets.len(), 2);

        // Should get second and third occurrences
        assert_eq!(snippets[0].fragment(), "d rust e f");
        assert_eq!(snippets[1].fragment(), "g rust h i");
    }

    #[test]
    fn test_snippets_empty_fragments() {
        let text = "a b c d e f g";

        let terms = BTreeMap::new();

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            10,
            None,
            None,
        );

        // No matching terms, no fragments
        assert_eq!(fragments.len(), 0);

        let snippets = select_top_fragments(&fragments[..], text, 5, 0, SnippetSortOrder::Position);
        assert_eq!(snippets.len(), 0);
    }

    fn check_paging_consistency(
        fragments: &[super::FragmentCandidate],
        text: &str,
        sort_order: super::SnippetSortOrder,
        expected_order: Vec<&str>,
    ) {
        // Get all snippets, sorted by the given order, to establish the ground truth.
        let all_snippets: Vec<_> = select_top_fragments(fragments, text, 0, 0, sort_order)
            .into_iter()
            .map(|s| s.to_html())
            .collect();

        assert_eq!(all_snippets, expected_order);

        // Now, page through the snippets and check if we get the same sequence.
        let mut paged_snippets = Vec::new();
        for i in 0..all_snippets.len() {
            let page: Vec<_> = select_top_fragments(fragments, text, 1, i, sort_order)
                .into_iter()
                .map(|s| s.to_html())
                .collect();
            assert_eq!(page.len(), 1);
            paged_snippets.push(page[0].clone());
        }

        assert_eq!(paged_snippets, all_snippets);
    }

    #[test]
    fn test_snippet_generator_paging_is_consistent() {
        // Scores where a > b > c > d
        let terms = btreemap! {
            String::from("a") => 4.0,
            String::from("b") => 3.0,
            String::from("c") => 2.0,
            String::from("d") => 1.0,
        };

        // Text where terms do not appear in score order.
        let text = "d c b a c d a b";

        let fragments = search_fragments(
            &mut From::from(SimpleTokenizer::default()),
            text,
            &terms,
            1, // max_num_chars
            None,
            None,
        );

        // Test position-based sorting and paging
        check_paging_consistency(
            &fragments,
            text,
            super::SnippetSortOrder::Position,
            vec![
                "<b>d</b>", "<b>c</b>", "<b>b</b>", "<b>a</b>", "<b>c</b>", "<b>d</b>", "<b>a</b>",
                "<b>b</b>",
            ],
        );

        // Test score-based sorting and paging
        check_paging_consistency(
            &fragments,
            text,
            super::SnippetSortOrder::Score,
            vec![
                "<b>a</b>", "<b>a</b>", "<b>b</b>", "<b>b</b>", "<b>c</b>", "<b>c</b>", "<b>d</b>",
                "<b>d</b>",
            ],
        );
    }

    #[test]
    fn test_snippet_generator_unlimited_fragments() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            let doc = doc!(text_field => "rust a b c rust d e f rust g h i rust j k l");
            index_writer.add_document(doc)?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("rust")?;
        let mut snippet_generator =
            SnippetGenerator::create(&searcher, &*query, text_field).unwrap();

        // Set to return unlimited snippets
        snippet_generator.set_snippets_limit(0);
        snippet_generator.set_max_num_chars(15);

        let text = "rust a b c rust d e f rust g h i rust j k l";
        let snippets = snippet_generator.snippets(text);

        // Should get multiple fragments (at least 3, depending on tokenization)
        assert!(snippets.len() >= 3);

        // All snippets should contain "rust"
        for snippet in &snippets {
            assert!(snippet.to_html().to_lowercase().contains("rust"));
        }

        Ok(())
    }

    #[test]
    fn test_snippet_generator_backward_compatibility() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            let doc = doc!(text_field => "rust a b c rust d e f rust g h i");
            index_writer.add_document(doc)?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("rust")?;
        let snippet_generator = SnippetGenerator::create(&searcher, &*query, text_field).unwrap();

        let text = "rust a b c rust d e f rust g h i";

        // Old API should still work and return single snippet
        let single_snippet = snippet_generator.snippet(text);
        assert!(!single_snippet.is_empty());
        assert!(single_snippet.to_html().contains("rust"));

        Ok(())
    }

    #[test]
    fn test_snippets_long_document_dispersed_matches() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            // Long document with matches at beginning, middle, and end
            let doc = doc!(text_field =>
                "rust is at the beginning. Lorem ipsum dolor sit amet consectetur adipiscing elit. \
                 Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. \
                 Here is rust in the middle of the document with more text around it. \
                 Ut enim ad minim veniam quis nostrud exercitation ullamco laboris. \
                 Nisi ut aliquip ex ea commodo consequat duis aute irure dolor. \
                 And finally rust appears at the end of this long document."
            );
            index_writer.add_document(doc)?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("rust")?;
        let mut snippet_generator =
            SnippetGenerator::create(&searcher, &*query, text_field).unwrap();

        snippet_generator.set_snippets_limit(3);
        snippet_generator.set_max_num_chars(50);
        snippet_generator.set_sort_order(SnippetSortOrder::Position);

        let text = "rust is at the beginning. Lorem ipsum dolor sit amet consectetur adipiscing \
                    elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Here \
                    is rust in the middle of the document with more text around it. Ut enim ad \
                    minim veniam quis nostrud exercitation ullamco laboris. Nisi ut aliquip ex ea \
                    commodo consequat duis aute irure dolor. And finally rust appears at the end \
                    of this long document.";
        let snippets = snippet_generator.snippets(text);

        // Should get 3 fragments for the 3 matches
        assert_eq!(snippets.len(), 3);

        // Verify they appear in document order
        // First fragment should contain "beginning"
        assert!(snippets[0].fragment().contains("beginning"));
        // Second fragment should contain "middle"
        assert!(snippets[1].fragment().contains("middle"));
        // Third fragment should contain "end"
        assert!(snippets[2].fragment().contains("end"));

        Ok(())
    }

    fn snippet_generator_for_text(text: &str, query: &str) -> SnippetGenerator {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(text_field => text)).unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query(query).unwrap();
        SnippetGenerator::create(&searcher, &*query, text_field).unwrap()
    }

    #[test]
    fn test_snippets_limit_less_than_fragments() {
        let text = "rust a rust b rust c rust d";
        let mut snippet_generator = snippet_generator_for_text(text, "rust");

        // Request 4 snippets, but limit matches to 2, with an offset of 1.
        snippet_generator.set_snippets_limit(4);
        snippet_generator.set_max_num_chars(10);
        snippet_generator.set_matches_limit(2);
        snippet_generator.set_matches_offset(1);
        snippet_generator.set_sort_order(SnippetSortOrder::Position);

        let snippets = snippet_generator.snippets(text);

        // We get 2 snippets, corresponding to the 2nd and 3rd matches.
        assert_eq!(snippets.len(), 2);
        assert_eq!(snippets[0].to_html(), "<b>rust</b> b");
        assert_eq!(snippets[1].to_html(), "<b>rust</b> c");
    }

    #[test]
    fn test_snippets_offset_changes_selection() {
        let text = "rust rust a b c rust d e f";
        let mut snippet_generator = snippet_generator_for_text(text, "rust");

        // Request 1 snippet, but skip the first two matches.
        snippet_generator.set_snippets_limit(1);
        snippet_generator.set_max_num_chars(15);
        snippet_generator.set_matches_offset(2);
        snippet_generator.set_sort_order(SnippetSortOrder::Position);

        let snippets = snippet_generator.snippets(text);

        // We should get 1 snippet.
        assert_eq!(snippets.len(), 1);
        // And it should be the one from the 3rd match, not the first fragment which has 2 matches.
        assert_eq!(snippets[0].to_html(), "<b>rust</b> d e f");
        assert!(!snippets[0].fragment().contains("rust rust"));
    }
}
