//! Span queries: slots of arbitrary term sources, matched at word positions
//! that keep a maximum distance between consecutive slots, or that all fit in
//! one window. See [`SpanQuery`].
//!
//! Each slot's matching terms are read with positions and unioned into one
//! positioned posting list, the way
//! [`RegexPhraseQuery`](crate::query::RegexPhraseQuery) does for regexes. The
//! slots are intersected by document, then the positions of each candidate
//! document are checked. With `n` positions over all `k` slots of a document:
//!
//! - Ordered sequence: one forward sweep, linear in `n`.
//! - Unordered sequence: a forward and a backward sweep, linear in `n`, keep the positions that lie
//!   on a match of the relaxed problem where a word may fill several slots. If no kept position can
//!   serve two non-adjacent slots (checked with one sort), the relaxed answer is exact. Otherwise
//!   an exact search over the kept positions runs, memoizing failed states. It is exponential in
//!   `k` in the worst case: the problem can encode job interval selection, which is NP-complete.
//! - Window: one sweep over the merged positions, keeping a slot/position matching up to date.
//!   Worst case: `O((n + k) * k * (max_span + 1))`.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use tantivy_fst::Automaton;

use super::phrase_query::RegexPhraseWeight;
use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
use crate::index::SegmentReader;
use crate::postings::{Postings, TermInfo};
use crate::query::explanation::does_not_match;
use crate::query::union::SimpleUnion;
use crate::query::{
    AutomatonWeight, ConstScorer, EmptyScorer, EnableScoring, Explanation, Intersection, Query,
    Scorer, Weight,
};
use crate::schema::{Field, IndexRecordOption, Term};
use crate::{DocId, Score, TantivyError};

/// The terms of one slot of a [`SpanQuery`].
pub trait SpanTermSource: Send + Sync + 'static {
    /// The field whose terms this source matches. It must be the field of the
    /// [`SpanQuery`] that holds the source.
    fn field(&self) -> Field;

    /// The term infos of the terms of [`SpanTermSource::field`] that this
    /// source matches in `reader`'s segment.
    fn match_term_infos(&self, reader: &SegmentReader) -> crate::Result<Vec<TermInfo>>;

    /// Passes the terms known before searching to `visitor`, as
    /// [`Query::query_terms`] does. Sources that only know their terms per
    /// segment (automata) pass none.
    fn query_terms<'a>(&'a self, _visitor: &mut dyn FnMut(&'a Term, bool)) {}
}

/// A slot matching one exact term.
#[derive(Clone, Debug)]
pub struct ExactTermSource {
    term: Term,
}

impl ExactTermSource {
    /// Creates a slot matching `term`.
    pub fn new(term: Term) -> ExactTermSource {
        ExactTermSource { term }
    }
}

impl SpanTermSource for ExactTermSource {
    fn field(&self) -> Field {
        self.term.field()
    }

    fn match_term_infos(&self, reader: &SegmentReader) -> crate::Result<Vec<TermInfo>> {
        let inverted_index = reader.inverted_index(self.term.field())?;
        Ok(inverted_index
            .get_term_info(&self.term)?
            .into_iter()
            .collect())
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        visitor(&self.term, true);
    }
}

/// A slot matching every term accepted by an automaton, e.g. the one of a
/// [`FuzzyTermQuery`](crate::query::FuzzyTermQuery) or a regex.
pub struct AutomatonTermSource<A> {
    field: Field,
    weight: AutomatonWeight<A>,
}

impl<A> AutomatonTermSource<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    /// Creates a slot matching the terms of `field` accepted by `automaton`.
    pub fn new<IntoArcA: Into<Arc<A>>>(field: Field, automaton: IntoArcA) -> Self {
        AutomatonTermSource {
            field,
            weight: AutomatonWeight::new(field, automaton),
        }
    }

    /// Creates a slot matching the terms under a JSON path accepted by
    /// `automaton`.
    pub fn new_for_json_path<IntoArcA: Into<Arc<A>>>(
        field: Field,
        automaton: IntoArcA,
        json_path_bytes: &[u8],
    ) -> Self {
        AutomatonTermSource {
            field,
            weight: AutomatonWeight::new_for_json_path(field, automaton, json_path_bytes),
        }
    }
}

impl<A> SpanTermSource for AutomatonTermSource<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    fn field(&self) -> Field {
        self.field
    }

    fn match_term_infos(&self, reader: &SegmentReader) -> crate::Result<Vec<TermInfo>> {
        self.weight.get_match_term_infos(reader)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SpanMode {
    /// `gaps[i]` is the maximum number of words between slots `i` and `i + 1`.
    Sequence {
        gaps: Vec<u32>,
        ordered: bool,
    },
    Window {
        max_span: u32,
    },
}

/// Matches documents where every slot matches a word of its own, the slots
/// being one of:
///
/// - a sequence, built with [`SpanQuery::new`] and [`SpanQuery::then_within`]: each slot is at most
///   a given number of words away from the previous one (0 = adjacent), in query order, or, after
///   [`SpanQuery::set_ordered`]`(false)`, with each consecutive pair in either order;
/// - a window, built with [`SpanQuery::window`]: in any order, the last position at most `max_span`
///   after the first.
///
/// A slot is any [`SpanTermSource`]: an [`ExactTermSource`], an
/// [`AutomatonTermSource`] or a custom source. Two slots never match the same
/// word. The field must be indexed with positions, and every slot must be on
/// the query's field.
///
/// Matching documents get a constant score (the boost), like the automaton
/// queries: the terms of an automaton slot are only known per segment, so
/// there is no index-wide document frequency to weigh them with. Combine it
/// with term queries in a [`BooleanQuery`](crate::query::BooleanQuery) to rank
/// the matches.
///
/// ```rust
/// use std::sync::Arc;
///
/// use tantivy::collector::Count;
/// use tantivy::query::{AutomatonTermSource, ExactTermSource, FuzzyTermQuery, SpanQuery};
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, IndexWriter, Term};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let body = schema_builder.add_text_field("body", TEXT);
/// let index = Index::create_in_ram(schema_builder.build());
/// let mut writer: IndexWriter = index.writer(15_000_000)?;
/// writer.add_document(doc!(body => "the quick brown fox jumps over the lazy dog"))?;
/// writer.add_document(doc!(body => "the dog jumps over the quick fox"))?;
/// writer.commit()?;
/// let searcher = index.reader()?.searcher();
///
/// let exact = |word: &str| Arc::new(ExactTermSource::new(Term::from_field_text(body, word)));
/// // "fox", then "jumps" right after it, then "dgo" (up to one edit) within 3 words.
/// let fuzzy = FuzzyTermQuery::new(Term::from_field_text(body, "dgo"), 1, true);
/// let query = SpanQuery::new(body, exact("fox"))
///     .then_within(0, exact("jumps"))
///     .then_within(3, Arc::new(AutomatonTermSource::new(body, fuzzy.automaton()?)));
/// assert_eq!(searcher.search(&query, &Count)?, 1);
///
/// // "quick" and "fox" at most one word apart, in either order.
/// let mut query = SpanQuery::new(body, exact("fox")).then_within(1, exact("quick"));
/// query.set_ordered(false);
/// assert_eq!(searcher.search(&query, &Count)?, 2);
///
/// // "dog", "fox" and "quick" in any order, spanning at most 6 positions.
/// let query = SpanQuery::window(body, vec![exact("dog"), exact("fox"), exact("quick")], 5);
/// assert_eq!(searcher.search(&query, &Count)?, 1);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SpanQuery {
    field: Field,
    slots: Vec<Arc<dyn SpanTermSource>>,
    mode: SpanMode,
}

impl std::fmt::Debug for SpanQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpanQuery")
            .field("field", &self.field)
            .field("num_slots", &self.slots.len())
            .field("mode", &self.mode)
            .finish()
    }
}

impl SpanQuery {
    /// Starts an ordered sequence on `field` with its first slot.
    pub fn new(field: Field, first_slot: Arc<dyn SpanTermSource>) -> SpanQuery {
        SpanQuery {
            field,
            slots: vec![first_slot],
            mode: SpanMode::Sequence {
                gaps: Vec::new(),
                ordered: true,
            },
        }
    }

    /// A window query: every slot at its own position, in any order, the last
    /// position at most `max_span` after the first (`max_span` = 0 fits one
    /// word only).
    ///
    /// # Panics
    ///
    /// If `slots` is empty.
    pub fn window(field: Field, slots: Vec<Arc<dyn SpanTermSource>>, max_span: u32) -> SpanQuery {
        assert!(!slots.is_empty(), "A span query needs at least one slot");
        SpanQuery {
            field,
            slots,
            mode: SpanMode::Window { max_span },
        }
    }

    /// Appends a slot at most `max_words_between` words away from the previous
    /// slot.
    ///
    /// # Panics
    ///
    /// On a window query, which has no per-slot distance.
    pub fn then_within(mut self, max_words_between: u32, slot: Arc<dyn SpanTermSource>) -> Self {
        let SpanMode::Sequence { gaps, .. } = &mut self.mode else {
            panic!("then_within applies to sequences; a window query takes its slots at creation");
        };
        gaps.push(max_words_between);
        self.slots.push(slot);
        self
    }

    /// Ordered (the default): the slots must appear in query order.
    /// Unordered: each consecutive pair of slots may appear in either order.
    ///
    /// # Panics
    ///
    /// On a window query, which is always unordered.
    pub fn set_ordered(&mut self, ordered: bool) {
        let SpanMode::Sequence {
            ordered: ordered_mut,
            ..
        } = &mut self.mode
        else {
            panic!("set_ordered applies to sequences; a window query is always unordered");
        };
        *ordered_mut = ordered;
    }
}

impl Query for SpanQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let field_entry = enable_scoring.schema().get_field_entry(self.field);
        let has_positions = field_entry
            .field_type()
            .get_index_record_option()
            .map(IndexRecordOption::has_positions)
            .unwrap_or(false);
        if !has_positions {
            return Err(TantivyError::SchemaError(format!(
                "Applied span query on field {:?}, which does not have positions indexed",
                field_entry.name()
            )));
        }
        if let Some(slot) = self.slots.iter().find(|slot| slot.field() != self.field) {
            return Err(TantivyError::InvalidArgument(format!(
                "Span query on field {:?} has a slot on field {:?}",
                field_entry.name(),
                enable_scoring.schema().get_field_name(slot.field())
            )));
        }
        Ok(Box::new(SpanWeight {
            field: self.field,
            slots: self.slots.clone(),
            mode: self.mode.clone(),
        }))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        for slot in &self.slots {
            slot.query_terms(visitor);
        }
    }
}

struct SpanWeight {
    field: Field,
    slots: Vec<Arc<dyn SpanTermSource>>,
    mode: SpanMode,
}

impl Weight for SpanWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inverted_index = reader.inverted_index(self.field)?;
        let mut unions = Vec::with_capacity(self.slots.len());
        for slot in &self.slots {
            let term_infos = slot.match_term_infos(reader)?;
            if term_infos.is_empty() {
                return Ok(Box::new(EmptyScorer));
            }
            unions.push(RegexPhraseWeight::get_union_from_term_infos(
                &term_infos,
                reader,
                &inverted_index,
            )?);
        }
        if unions.len() == 1 {
            // Any position of a lone slot is a match.
            let union = unions.pop().expect("one slot");
            return Ok(Box::new(ConstScorer::new(union, boost)));
        }
        Ok(Box::new(SpanScorer::new(
            unions,
            self.mode.clone(),
            reader.max_doc(),
            boost,
        )))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.doc() > doc || scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new(
            "SpanScorer, constant score",
            scorer.score(),
        ))
    }
}

type SlotUnion = SimpleUnion<Box<dyn Postings + 'static>>;

/// A slot's postings, remembering the slot: the intersection reorders them.
struct SlotPostings {
    slot: usize,
    postings: SlotUnion,
}

impl DocSet for SlotPostings {
    fn advance(&mut self) -> DocId {
        self.postings.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.postings.seek(target)
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }

    fn cost(&self) -> u64 {
        self.postings.cost()
    }
}

struct SpanScorer {
    intersection: Intersection<SlotPostings, SlotPostings>,
    /// `ords[slot]`: the slot's index in the intersection.
    ords: Vec<usize>,
    positions: Vec<Vec<u32>>,
    mode: SpanMode,
    matcher: SpanMatcher,
    boost: Score,
}

impl SpanScorer {
    fn new(unions: Vec<SlotUnion>, mode: SpanMode, num_docs: u32, boost: Score) -> SpanScorer {
        let num_slots = unions.len();
        let slot_postings = unions
            .into_iter()
            .enumerate()
            .map(|(slot, postings)| SlotPostings { slot, postings })
            .collect();
        let mut intersection = Intersection::new(slot_postings, num_docs);
        let mut ords = vec![0; num_slots];
        for ord in 0..num_slots {
            ords[intersection.docset_mut_specialized(ord).slot] = ord;
        }
        let mut scorer = SpanScorer {
            intersection,
            ords,
            positions: vec![Vec::new(); num_slots],
            mode,
            matcher: SpanMatcher::default(),
            boost,
        };
        if scorer.doc() != TERMINATED && !scorer.span_matches() {
            scorer.advance();
        }
        scorer
    }

    fn span_matches(&mut self) -> bool {
        for (slot, buffer) in self.positions.iter_mut().enumerate() {
            self.intersection
                .docset_mut_specialized(self.ords[slot])
                .postings
                .positions(buffer);
        }
        self.matcher.matches(&self.positions, &self.mode)
    }
}

impl DocSet for SpanScorer {
    fn advance(&mut self) -> DocId {
        loop {
            let doc = self.intersection.advance();
            if doc == TERMINATED || self.span_matches() {
                return doc;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc());
        let doc = self.intersection.seek(target);
        if doc == TERMINATED || self.span_matches() {
            return doc;
        }
        self.advance()
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        let seek_result = self.intersection.seek_danger(target);
        if seek_result != SeekDangerResult::Found {
            return seek_result;
        }
        if self.span_matches() {
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn doc(&self) -> DocId {
        self.intersection.doc()
    }

    fn size_hint(&self) -> u32 {
        // As for phrases: far fewer documents pass the position check than
        // contain every slot.
        self.intersection.size_hint() / (10 * self.positions.len() as u32)
    }

    fn cost(&self) -> u64 {
        self.intersection.size_hint() as u64 * 10 * self.positions.len() as u64
    }
}

impl Scorer for SpanScorer {
    fn score(&mut self) -> Score {
        self.boost
    }
}

/// The position check of one document, with its scratch reused across
/// documents. `positions[slot]` is sorted and deduplicated.
#[derive(Default)]
struct SpanMatcher {
    /// Per slot, the positions where a (relaxed) match can place it.
    reach: Vec<Vec<u32>>,
    scratch: Vec<u32>,
    /// `(position, slot)` of every reachable position, sorted.
    occupants: Vec<(u32, usize)>,
    /// `(position, last slot that can take it)`, sorted.
    last_slot: Vec<(u32, usize)>,
    /// Positions taken by the exact search, one per placed slot.
    used: Vec<u32>,
    /// States of the exact search known to fail: (slot, position, taken
    /// positions that a later slot could still want).
    failed: HashSet<(usize, u32, Vec<u32>)>,
    window: WindowMatcher,
}

/// Keeps the candidates having an anchor at word distance `1..=gap + 1`,
/// strictly before them when `ordered`.
fn filter_reachable(
    anchors: &[u32],
    candidates: &[u32],
    gap: u32,
    ordered: bool,
    out: &mut Vec<u32>,
) {
    out.clear();
    let reach = u64::from(gap) + 1;
    let mut j = 0;
    for &candidate in candidates {
        let lo = u64::from(candidate).saturating_sub(reach);
        while j < anchors.len() && u64::from(anchors[j]) < lo {
            j += 1;
        }
        let Some(&anchor) = anchors.get(j) else {
            break;
        };
        let reachable = if anchor < candidate {
            true
        } else if ordered {
            false
        } else {
            let after = if anchor == candidate {
                anchors.get(j + 1).copied()
            } else {
                Some(anchor)
            };
            after.is_some_and(|after| u64::from(after) <= u64::from(candidate) + reach)
        };
        if reachable {
            out.push(candidate);
        }
    }
}

impl SpanMatcher {
    fn matches(&mut self, positions: &[Vec<u32>], mode: &SpanMode) -> bool {
        match mode {
            SpanMode::Window { max_span } => self.window.matches(positions, *max_span),
            SpanMode::Sequence { gaps, ordered } => {
                self.sequence_matches(positions, gaps, *ordered)
            }
        }
    }

    fn sequence_matches(&mut self, positions: &[Vec<u32>], gaps: &[u32], ordered: bool) -> bool {
        let num_slots = positions.len();
        self.reach.resize_with(num_slots, Vec::new);
        self.reach[0].clear();
        self.reach[0].extend_from_slice(&positions[0]);
        for slot in 1..num_slots {
            let (done, rest) = self.reach.split_at_mut(slot);
            filter_reachable(
                &done[slot - 1],
                &positions[slot],
                gaps[slot - 1],
                ordered,
                &mut rest[0],
            );
            if rest[0].is_empty() {
                return false;
            }
        }
        // In order, positions strictly increase: they are distinct.
        if ordered {
            return true;
        }
        // Keep the positions that also reach the last slot. The relation is
        // symmetric, so every position left lies on a relaxed match.
        for slot in (0..num_slots - 1).rev() {
            filter_reachable(
                &self.reach[slot + 1],
                &self.reach[slot],
                gaps[slot],
                false,
                &mut self.scratch,
            );
            std::mem::swap(&mut self.reach[slot], &mut self.scratch);
        }
        // Adjacent slots never share a word (distance >= 1). Unless a word can
        // fill two slots further apart, every relaxed match is a match.
        self.occupants.clear();
        for (slot, list) in self.reach.iter().enumerate() {
            self.occupants.extend(list.iter().map(|&p| (p, slot)));
        }
        self.occupants.sort_unstable();
        self.last_slot.clear();
        let mut shared = false;
        for group in self.occupants.chunk_by(|a, b| a.0 == b.0) {
            let (first, last) = (group[0].1, group[group.len() - 1].1);
            shared |= last - first >= 2;
            self.last_slot.push((group[0].0, last));
        }
        if !shared {
            return true;
        }
        // Not enough distinct positions for the slots at all.
        if !self.window.matches(&self.reach, u32::MAX) {
            return false;
        }
        self.failed.clear();
        for i in 0..self.reach[0].len() {
            let start = self.reach[0][i];
            self.used.clear();
            self.used.push(start);
            if self.place_from(0, start, gaps) {
                return true;
            }
        }
        false
    }

    /// Exact search: with `slot` at `position` (and the slots before it in
    /// `used`), can the later slots be placed at distinct positions?
    fn place_from(&mut self, slot: usize, position: u32, gaps: &[u32]) -> bool {
        let next = slot + 1;
        if next == gaps.len() + 1 {
            return true;
        }
        // Only taken positions that a later slot could want shape the outcome.
        let mut relevant: Vec<u32> = self
            .used
            .iter()
            .copied()
            .filter(|&used| self.last_slot_of(used) > slot)
            .collect();
        relevant.sort_unstable();
        let state = (slot, position, relevant);
        if self.failed.contains(&state) {
            return false;
        }
        let reach = u64::from(gaps[slot]) + 1;
        let lo = u64::from(position).saturating_sub(reach);
        let hi = u64::from(position) + reach;
        let mut idx = self.reach[next].partition_point(|&q| u64::from(q) < lo);
        while let Some(&candidate) = self.reach[next].get(idx) {
            if u64::from(candidate) > hi {
                break;
            }
            idx += 1;
            if candidate == position || self.used.contains(&candidate) {
                continue;
            }
            self.used.push(candidate);
            if self.place_from(next, candidate, gaps) {
                return true;
            }
            self.used.pop();
        }
        self.failed.insert(state);
        false
    }

    fn last_slot_of(&self, position: u32) -> usize {
        let idx = self.last_slot.partition_point(|&(p, _)| p < position);
        self.last_slot[idx].1
    }
}

const UNMATCHED: usize = usize::MAX;

/// Window mode's check, with its scratch reused across documents.
///
/// One sweep over the slots' occurrences in position order. The window keeps,
/// per slot, the positions it can take, and a slot/position matching that is
/// kept up to date incrementally: a position leaving the window frees its
/// slot, and only free slots are augmented (Kuhn's algorithm), so a matching
/// is never rebuilt from scratch.
#[derive(Default)]
struct WindowMatcher {
    /// (position, slot) of every slot occurrence, sorted.
    entries: Vec<(u32, usize)>,
    /// Position number (0.. over the distinct positions) of each entry.
    position_ids: Vec<usize>,
    /// Occurrences per slot inside the window.
    coverage: Vec<u32>,
    /// Per slot: the numbers of the positions it can take in the window.
    in_window: Vec<VecDeque<usize>>,
    /// Per position number: the slot it is matched to, or `UNMATCHED`.
    owner: Vec<usize>,
    /// Per slot: its matched position number, or `UNMATCHED`.
    taken: Vec<usize>,
    /// Visit marks of the current augmenting search (`generation`).
    stamp: Vec<u32>,
    generation: u32,
    /// Augmenting path: (slot, next candidate index in its `in_window`).
    stack: Vec<(usize, usize)>,
}

impl WindowMatcher {
    /// True when every slot gets its own position inside one window of at
    /// most `max_span`. `positions[slot]` is the slot's sorted positions.
    fn matches(&mut self, positions: &[Vec<u32>], max_span: u32) -> bool {
        let num_slots = positions.len();
        self.entries.clear();
        for (slot, list) in positions.iter().enumerate() {
            self.entries.extend(list.iter().map(|&p| (p, slot)));
        }
        self.entries.sort_unstable();
        self.position_ids.clear();
        let mut next_id = 0;
        for i in 0..self.entries.len() {
            if i > 0 && self.entries[i - 1].0 != self.entries[i].0 {
                next_id += 1;
            }
            self.position_ids.push(next_id);
        }
        let position_count = next_id + 1;
        self.coverage.clear();
        self.coverage.resize(num_slots, 0);
        self.in_window.resize_with(num_slots, VecDeque::new);
        self.in_window.iter_mut().for_each(VecDeque::clear);
        self.owner.clear();
        self.owner.resize(position_count, UNMATCHED);
        self.taken.clear();
        self.taken.resize(num_slots, UNMATCHED);
        self.stamp.clear();
        self.stamp.resize(position_count, 0);
        self.generation = 0;

        let len = self.entries.len();
        let (mut covered, mut distinct, mut matched) = (0usize, 0usize, 0usize);
        let mut left = 0;
        for right in 0..len {
            let (position, slot) = self.entries[right];
            let id = self.position_ids[right];
            covered += usize::from(self.coverage[slot] == 0);
            self.coverage[slot] += 1;
            self.in_window[slot].push_back(id);
            if right == 0 || self.position_ids[right - 1] != id {
                distinct += 1;
            }
            while position - self.entries[left].0 > max_span {
                let gone = self.entries[left].1;
                let gone_id = self.position_ids[left];
                self.coverage[gone] -= 1;
                covered -= usize::from(self.coverage[gone] == 0);
                self.in_window[gone].pop_front();
                if self.position_ids[left + 1] != gone_id {
                    distinct -= 1; // the position's last entry left
                }
                let holder = self.owner[gone_id];
                if holder != UNMATCHED {
                    self.owner[gone_id] = UNMATCHED;
                    self.taken[holder] = UNMATCHED;
                    matched -= 1;
                }
                left += 1;
            }
            // Every entry of this position must be in before deciding.
            if right + 1 < len && self.position_ids[right + 1] == id {
                continue;
            }
            if covered < num_slots || distinct < num_slots {
                continue;
            }
            for free_slot in 0..num_slots {
                if self.taken[free_slot] == UNMATCHED {
                    if !self.augment(free_slot) {
                        break;
                    }
                    matched += 1;
                }
            }
            if matched == num_slots {
                return true;
            }
        }
        false
    }

    /// Kuhn's augmenting path from the free slot `start`, iterative; on
    /// success the path is flipped and `start` is matched.
    fn augment(&mut self, start: usize) -> bool {
        self.generation += 1;
        let generation = self.generation;
        self.stack.clear();
        self.stack.push((start, 0));
        while let Some(&(slot, i)) = self.stack.last() {
            let Some(&id) = self.in_window[slot].get(i) else {
                self.stack.pop();
                continue;
            };
            self.stack.last_mut().expect("non-empty").1 = i + 1;
            if self.stamp[id] == generation {
                continue;
            }
            self.stamp[id] = generation;
            let holder = self.owner[id];
            if holder == UNMATCHED {
                // Each frame takes the position it tried last.
                for &(s, next) in &self.stack {
                    let taken = self.in_window[s][next - 1];
                    self.owner[taken] = s;
                    self.taken[s] = taken;
                }
                return true;
            }
            self.stack.push((holder, 0));
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{
        AutomatonTermSource, ExactTermSource, SpanMatcher, SpanMode, SpanQuery, SpanTermSource,
    };
    use crate::collector::{Count, DocSetCollector};
    use crate::query::{EnableScoring, FuzzyTermQuery, Query};
    use crate::schema::{Field, Schema, TEXT};
    use crate::{DocAddress, Index, IndexWriter, TantivyError, Term};

    fn build(lines: &[&str]) -> crate::Result<(Index, Field)> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        for line in lines {
            writer.add_document(doc!(text => *line))?;
        }
        writer.commit()?;
        Ok((index, text))
    }

    fn exact(field: Field, word: &str) -> Arc<dyn SpanTermSource> {
        Arc::new(ExactTermSource::new(Term::from_field_text(field, word)))
    }

    fn sequence(field: Field, words: &[&str], gaps: &[u32], ordered: bool) -> SpanQuery {
        let mut query = SpanQuery::new(field, exact(field, words[0]));
        for (word, &gap) in words[1..].iter().zip(gaps) {
            query = query.then_within(gap, exact(field, word));
        }
        query.set_ordered(ordered);
        query
    }

    fn window(field: Field, words: &[&str], max_span: u32) -> SpanQuery {
        let slots = words.iter().map(|word| exact(field, word)).collect();
        SpanQuery::window(field, slots, max_span)
    }

    fn search(index: &Index, query: &dyn Query) -> crate::Result<Vec<u32>> {
        let searcher = index.reader()?.searcher();
        let mut docs: Vec<u32> = searcher
            .search(query, &DocSetCollector)?
            .into_iter()
            .map(|addr| addr.doc_id)
            .collect();
        docs.sort_unstable();
        Ok(docs)
    }

    #[test]
    fn adjacent_and_gap_counting() -> crate::Result<()> {
        let (index, f) = build(&["a b", "a x b", "a x y b", "a x y z b"])?;
        assert_eq!(
            search(&index, &sequence(f, &["a", "b"], &[0], true))?,
            vec![0]
        );
        assert_eq!(
            search(&index, &sequence(f, &["a", "b"], &[2], true))?,
            vec![0, 1, 2]
        );
        Ok(())
    }

    #[test]
    fn per_gap_limits() -> crate::Result<()> {
        // doc 0: a-1-b-3-c, doc 1: a-3-b-0-c
        let (index, f) = build(&["a x b x x x c", "a x x x b c"])?;
        let query = sequence(f, &["a", "b", "c"], &[2, 5], true);
        assert_eq!(search(&index, &query)?, vec![0]);
        Ok(())
    }

    #[test]
    fn ordered_vs_unordered() -> crate::Result<()> {
        let (index, f) = build(&["b a", "a b"])?;
        assert_eq!(
            search(&index, &sequence(f, &["a", "b"], &[0], true))?,
            vec![1]
        );
        assert_eq!(
            search(&index, &sequence(f, &["a", "b"], &[0], false))?,
            vec![0, 1]
        );
        Ok(())
    }

    #[test]
    fn one_word_cannot_fill_two_slots() -> crate::Result<()> {
        let (index, f) = build(&["a b", "a a", "a b a", "b a"])?;
        assert_eq!(
            search(&index, &sequence(f, &["a", "a"], &[3], false))?,
            vec![1, 2]
        );
        // Non-adjacent slots: the first and the last "a" need two words.
        assert_eq!(
            search(&index, &sequence(f, &["a", "b", "a"], &[3, 3], false))?,
            vec![2]
        );
        Ok(())
    }

    #[test]
    fn largest_gap_does_not_overflow() -> crate::Result<()> {
        let (index, f) = build(&["b x x a", "a", "a x b"])?;
        assert_eq!(
            search(&index, &sequence(f, &["a", "b"], &[u32::MAX], false))?,
            vec![0, 2]
        );
        assert_eq!(
            search(&index, &sequence(f, &["a", "b"], &[u32::MAX], true))?,
            vec![2]
        );
        assert_eq!(
            search(
                &index,
                &sequence(f, &["a", "b", "a"], &[u32::MAX; 2], false)
            )?,
            Vec::<u32>::new()
        );
        Ok(())
    }

    #[test]
    fn automaton_slot() -> crate::Result<()> {
        let (index, f) = build(&["japan korea", "japon x korea", "china korea"])?;
        let fuzzy = FuzzyTermQuery::new(Term::from_field_text(f, "japan"), 1, true);
        let slot = Arc::new(AutomatonTermSource::new(f, fuzzy.automaton()?));
        let query = SpanQuery::new(f, slot).then_within(1, exact(f, "korea"));
        assert_eq!(search(&index, &query)?, vec![0, 1]);
        Ok(())
    }

    #[test]
    fn single_slot_matches_every_occurrence() -> crate::Result<()> {
        let (index, f) = build(&["a b", "b", "x a"])?;
        assert_eq!(
            search(&index, &SpanQuery::new(f, exact(f, "a")))?,
            vec![0, 2]
        );
        assert_eq!(search(&index, &window(f, &["a"], 0))?, vec![0, 2]);
        Ok(())
    }

    #[test]
    fn window_mode_any_order_distinct_positions() -> crate::Result<()> {
        let (index, f) = build(&[
            "a b",
            "b a",
            "b x x a",
            "c x a b",
            "a x a",
            "a",
            "b a x x x c",
        ])?;
        assert_eq!(
            search(&index, &window(f, &["a", "b"], 1))?,
            vec![0, 1, 3, 6]
        );
        assert_eq!(
            search(&index, &window(f, &["b", "a"], 3))?,
            vec![0, 1, 2, 3, 6]
        );
        assert_eq!(search(&index, &window(f, &["a", "b", "c"], 3))?, vec![3]);
        assert_eq!(search(&index, &window(f, &["a", "b", "c"], 5))?, vec![3, 6]);
        assert!(search(&index, &window(f, &["a", "a"], 1))?.is_empty());
        assert_eq!(search(&index, &window(f, &["a", "a"], 2))?, vec![4]);
        Ok(())
    }

    #[test]
    fn window_mode_shared_positions_need_a_matching() -> crate::Result<()> {
        // slot 0: any one-letter word ("a"~1), slot 1: "a".
        let (index, f) = build(&["a b", "a x", "a a", "b b"])?;
        let any_letter = FuzzyTermQuery::new(Term::from_field_text(f, "a"), 1, true);
        let slot = Arc::new(AutomatonTermSource::new(f, any_letter.automaton()?));
        let query = SpanQuery::window(f, vec![slot, exact(f, "a")], 1);
        assert_eq!(search(&index, &query)?, vec![0, 1, 2]);
        assert_eq!(search(&index, &window(f, &["a", "a"], 1))?, vec![2]);
        Ok(())
    }

    #[test]
    #[should_panic(expected = "then_within applies to sequences")]
    fn window_has_no_gaps() {
        let field = Field::from_field_id(0);
        let _ = window(field, &["a", "b"], 3).then_within(1, exact(field, "c"));
    }

    #[test]
    fn ordered_sweep_takes_a_later_start() -> crate::Result<()> {
        // The first "a" is too far from "b"; the second one reaches it.
        let (index, f) = build(&["a x x a b c", "a x x x b"])?;
        let query = sequence(f, &["a", "b", "c"], &[0, 0], true);
        assert_eq!(search(&index, &query)?, vec![0]);
        Ok(())
    }

    #[test]
    fn missing_slot_matches_nothing() -> crate::Result<()> {
        let (index, f) = build(&["a b"])?;
        assert!(search(&index, &sequence(f, &["a", "zzz"], &[5], true))?.is_empty());
        Ok(())
    }

    #[test]
    fn slot_on_another_field_is_rejected() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let body = schema_builder.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let searcher = index.reader()?.searcher();
        let fuzzy = FuzzyTermQuery::new(Term::from_field_text(body, "b"), 1, true);
        let queries = [
            SpanQuery::new(title, exact(title, "a")).then_within(1, exact(body, "b")),
            SpanQuery::new(
                title,
                Arc::new(AutomatonTermSource::new(body, fuzzy.automaton()?)),
            ),
        ];
        for query in queries {
            let result = query.weight(EnableScoring::disabled_from_searcher(&searcher));
            assert!(
                matches!(&result, Err(TantivyError::InvalidArgument(msg)) if msg.contains("\"body\"")),
                "{:?}",
                result.err()
            );
        }
        Ok(())
    }

    #[test]
    fn query_terms_lists_exact_slots() -> crate::Result<()> {
        let (_, f) = build(&[])?;
        let fuzzy = FuzzyTermQuery::new(Term::from_field_text(f, "c"), 1, true);
        let query = SpanQuery::new(f, exact(f, "a"))
            .then_within(1, Arc::new(AutomatonTermSource::new(f, fuzzy.automaton()?)))
            .then_within(1, exact(f, "b"));
        let mut terms = Vec::new();
        query.query_terms(&mut |term, positions| terms.push((term.clone(), positions)));
        assert_eq!(
            terms,
            vec![
                (Term::from_field_text(f, "a"), true),
                (Term::from_field_text(f, "b"), true)
            ]
        );
        Ok(())
    }

    #[test]
    fn explain_and_deletes() -> crate::Result<()> {
        let (index, f) = build(&["a b", "b x", "a x b", "a b"])?;
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.delete_term(Term::from_field_text(f, "x"));
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let query = sequence(f, &["a", "b"], &[1], true);
        assert_eq!(search(&index, &query)?, vec![0, 3]);
        assert_eq!(searcher.search(&query, &Count)?, 2);
        assert_eq!(query.count(&searcher)?, 2);
        let explanation = query.explain(&searcher, DocAddress::new(0, 3))?;
        assert_eq!(explanation.value(), 1.0);
        assert!(query.explain(&searcher, DocAddress::new(0, 1)).is_err());
        Ok(())
    }

    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// Every injective slot -> position assignment, brute force.
    fn reference(positions: &[Vec<u32>], mode: &SpanMode) -> bool {
        fn fits(chosen: &[u32], mode: &SpanMode) -> bool {
            match mode {
                SpanMode::Window { max_span } => {
                    let lo = chosen.iter().min().copied().unwrap_or(0);
                    let hi = chosen.iter().max().copied().unwrap_or(0);
                    hi - lo <= *max_span
                }
                SpanMode::Sequence { gaps, ordered } => {
                    chosen.windows(2).zip(gaps).all(|(pair, &gap)| {
                        (!*ordered || pair[0] < pair[1])
                            && u64::from(pair[0].abs_diff(pair[1])) <= u64::from(gap) + 1
                    })
                }
            }
        }
        fn place(
            slot: usize,
            positions: &[Vec<u32>],
            chosen: &mut Vec<u32>,
            mode: &SpanMode,
        ) -> bool {
            if slot == positions.len() {
                return fits(chosen, mode);
            }
            for &p in &positions[slot] {
                if chosen.contains(&p) {
                    continue;
                }
                chosen.push(p);
                if place(slot + 1, positions, chosen, mode) {
                    return true;
                }
                chosen.pop();
            }
            false
        }
        place(0, positions, &mut Vec::new(), mode)
    }

    fn random_mode(rng: &mut Rng, num_slots: usize) -> SpanMode {
        let kind = rng.below(3);
        let mut gap = || match rng.below(8) {
            0 => u32::MAX,
            n => (n - 1) as u32,
        };
        match kind {
            0 => SpanMode::Window {
                max_span: gap().min(6),
            },
            n => SpanMode::Sequence {
                gaps: (1..num_slots).map(|_| gap()).collect(),
                ordered: n == 1,
            },
        }
    }

    #[test]
    fn matcher_equals_brute_force() {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        let mut matcher = SpanMatcher::default();
        let mut checked = [0usize; 3];
        for _ in 0..20_000 {
            let num_slots = 1 + rng.below(5) as usize;
            let len = 1 + rng.below(12) as u32;
            // Each slot takes a random subset of the positions, shared ones too.
            let positions: Vec<Vec<u32>> = (0..num_slots)
                .map(|_| (0..len).filter(|_| rng.below(3) == 0).collect())
                .collect();
            if positions.iter().any(Vec::is_empty) {
                continue;
            }
            let mode = random_mode(&mut rng, num_slots);
            let expected = reference(&positions, &mode);
            assert_eq!(
                matcher.matches(&positions, &mode),
                expected,
                "{positions:?} {mode:?}"
            );
            match mode {
                SpanMode::Sequence { ordered: true, .. } => checked[0] += 1,
                SpanMode::Sequence { ordered: false, .. } => checked[1] += 1,
                SpanMode::Window { .. } => checked[2] += 1,
            }
        }
        assert!(checked.iter().all(|&count| count > 1000), "{checked:?}");
    }

    #[test]
    fn query_equals_brute_force() -> crate::Result<()> {
        const WORDS: [&str; 4] = ["a", "b", "c", "d"];
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let docs: Vec<Vec<&str>> = (0..60)
            .map(|_| {
                let len = 1 + rng.below(9) as usize;
                (0..len).map(|_| WORDS[rng.below(4) as usize]).collect()
            })
            .collect();
        let lines: Vec<String> = docs.iter().map(|words| words.join(" ")).collect();
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        let (index, f) = build(&lines)?;
        for _ in 0..300 {
            let num_slots = 1 + rng.below(4) as usize;
            let words: Vec<&str> = (0..num_slots)
                .map(|_| WORDS[rng.below(4) as usize])
                .collect();
            let mode = random_mode(&mut rng, num_slots);
            let query = match &mode {
                SpanMode::Window { max_span } => window(f, &words, *max_span),
                SpanMode::Sequence { gaps, ordered } => sequence(f, &words, gaps, *ordered),
            };
            let expected: Vec<u32> = (0..docs.len() as u32)
                .filter(|&doc| {
                    let positions: Vec<Vec<u32>> = words
                        .iter()
                        .map(|word| {
                            (0..docs[doc as usize].len() as u32)
                                .filter(|&p| docs[doc as usize][p as usize] == *word)
                                .collect()
                        })
                        .collect();
                    reference(&positions, &mode)
                })
                .collect();
            assert_eq!(search(&index, &query)?, expected, "{words:?} {mode:?}");
        }
        Ok(())
    }

    #[test]
    fn window_matcher_dense_broad_slot_is_fast() {
        // Three slots take every position (a broad automaton), two take every
        // sixth: no window of span 4 holds two of those, so every window is
        // matched and fails, with augmenting paths through the broad slots.
        let all: Vec<u32> = (0..2000).collect();
        let sparse: Vec<u32> = (0..2000).step_by(6).collect();
        let positions = vec![all.clone(), all.clone(), all, sparse.clone(), sparse];
        let mut matcher = SpanMatcher::default();
        let start = Instant::now();
        assert!(!matcher.matches(&positions, &SpanMode::Window { max_span: 4 }));
        assert!(matcher.matches(&positions, &SpanMode::Window { max_span: 6 }));
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "{:?}",
            start.elapsed()
        );
    }

    #[test]
    fn unordered_matcher_dense_slots_are_fast() {
        let mut matcher = SpanMatcher::default();
        let start = Instant::now();
        // Five broad slots over 2,000 positions, then a slot out of reach: a
        // plain backtracking search walks (2 * 5 + 2)^5 paths per start.
        let all: Vec<u32> = (0..2000).collect();
        let mut positions = vec![all; 5];
        positions.push(vec![3000]);
        let mode = SpanMode::Sequence {
            gaps: vec![5; 5],
            ordered: false,
        };
        assert!(!matcher.matches(&positions, &mode));
        // Ten identical slots over clusters of nine words, too far apart to
        // chain: each cluster is one slot short, which only shows once its
        // 9! orders have been tried, unless failed states are remembered.
        let clusters: Vec<u32> = (0..20u32)
            .flat_map(|cluster| (0..9).map(move |word| cluster * 100 + word))
            .collect();
        let positions = vec![clusters.clone(); 10];
        let mode = SpanMode::Sequence {
            gaps: vec![8; 9],
            ordered: false,
        };
        assert!(!matcher.matches(&positions, &mode));
        let mut positions = positions;
        positions.truncate(9);
        let mode = SpanMode::Sequence {
            gaps: vec![8; 8],
            ordered: false,
        };
        assert!(matcher.matches(&positions, &mode));
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "{:?}",
            start.elapsed()
        );
    }
}
