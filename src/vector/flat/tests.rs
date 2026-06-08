use std::ops::Bound;

use crate::collector::TopDocs;
use crate::index::SegmentId;
use crate::indexer::NoMergePolicy;
use crate::query::{AllQuery, Query, RangeQuery, TermQuery};
use crate::schema::{
    Field, IndexRecordOption, Metric, Schema, Term, Value, VectorOptions, FAST, INDEXED, STORED,
    TEXT,
};
use crate::vector::VectorColumnReader;
use crate::{DocAddress, Index, IndexWriter, Score, Searcher, TantivyDocument};

// ---- helpers shared across the metric tests ----

/// Search top-N via vector similarity, with the supplied filter query as
/// the candidate set.
fn search_top_n(
    searcher: &Searcher,
    query: &dyn Query,
    vec_field: Field,
    query_vec: Vec<f32>,
    k: usize,
) -> crate::Result<Vec<(Score, DocAddress)>> {
    searcher.search(
        query,
        &TopDocs::with_limit(k).order_by_similarity(vec_field, query_vec),
    )
}

/// Scores must come back in descending order.
fn assert_descending(hits: &[(Score, DocAddress)]) {
    for w in hits.windows(2) {
        assert!(
            w[0].0 >= w[1].0,
            "scores not descending: {:?} >= {:?}",
            w[0],
            w[1],
        );
    }
}

fn score_at(hits: &[(Score, DocAddress)], i: usize) -> Score {
    hits[i].0
}

/// Fetch the first stored text value for `field` at `doc_addr`.
fn stored_text(searcher: &Searcher, field: Field, doc_addr: DocAddress) -> crate::Result<String> {
    let doc = searcher.doc::<TantivyDocument>(doc_addr)?;
    Ok(doc
        .get_first(field)
        .and_then(|v| Value::as_str(&v))
        .expect("stored text value")
        .to_string())
}

// ---- L2: text filter, single vector field, multi-segment ----

#[test]
fn test_search_l2() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | STORED);
    let embed = schema_builder.add_vector_field("embedding", VectorOptions::new(3, Metric::L2));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    // Two segments, six docs total.
    let batches = [
        vec![
            ("alpha", [1.0f32, 0.0, 0.0]),
            ("alpha", [0.9, 0.1, 0.0]),
            ("beta", [0.0, 1.0, 0.0]),
        ],
        vec![
            ("beta", [0.0f32, 0.0, 1.0]),
            ("alpha", [-1.0, 0.0, 0.0]),
            ("alpha", [0.95, 0.05, 0.0]),
        ],
    ];
    for batch in &batches {
        for (cat, v) in batch {
            let mut doc = TantivyDocument::new();
            doc.add_text(category, cat);
            doc.add_vector(embed, v);
            writer.add_document(doc)?;
        }
        writer.commit()?;
    }

    let searcher = index.reader()?.searcher();
    let query_vec = vec![1.0f32, 0.0, 0.0];

    // Pure vector search: top-3 should all be alpha docs (point along +x).
    let hits = search_top_n(&searcher, &AllQuery, embed, query_vec.clone(), 3)?;
    assert_eq!(hits.len(), 3);
    assert_descending(&hits);
    // Exact match → -l2² == 0.
    assert!(score_at(&hits, 0).abs() < 1e-6, "top: {:?}", hits[0].0);
    for (_, addr) in &hits {
        assert_eq!(stored_text(&searcher, category, *addr)?, "alpha");
    }

    // Filtered: only beta docs.
    let beta = TermQuery::new(
        Term::from_field_text(category, "beta"),
        IndexRecordOption::Basic,
    );
    let hits = search_top_n(&searcher, &beta, embed, query_vec, 10)?;
    assert_eq!(hits.len(), 2);
    assert_descending(&hits);
    for (_, addr) in &hits {
        assert_eq!(stored_text(&searcher, category, *addr)?, "beta");
    }

    Ok(())
}

// ---- Cosine: u64 range filter, two vector fields of different dim ----

#[test]
fn test_search_cosine() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let group = schema_builder.add_u64_field("group", INDEXED | FAST | STORED);
    let small = schema_builder.add_vector_field("small", VectorOptions::new(2, Metric::Cosine));
    let big = schema_builder.add_vector_field("big", VectorOptions::new(4, Metric::Cosine));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    // (group, small, big) — five docs split across two segments.
    let docs = [
        (1u64, [1.0f32, 0.0], [1.0f32, 0.0, 0.0, 0.0]),
        (1, [0.9, 0.1], [0.9, 0.1, 0.0, 0.0]),
        (2, [0.0, 1.0], [0.0, 1.0, 0.0, 0.0]),
        (2, [-1.0, 0.0], [-1.0, 0.0, 0.0, 0.0]),
        (3, [0.0, -1.0], [0.0, 0.0, 0.0, 1.0]),
    ];
    for (i, (g, s, b)) in docs.iter().enumerate() {
        let mut doc = TantivyDocument::new();
        doc.add_u64(group, *g);
        doc.add_vector(small, s);
        doc.add_vector(big, b);
        writer.add_document(doc)?;
        if i == 2 {
            writer.commit()?;
        }
    }
    writer.commit()?;

    let searcher = index.reader()?.searcher();

    // Pure: closest to [1,0] in the small field — identical doc gets cos=1,
    // opposite-direction doc gets cos=-1.
    let hits = search_top_n(&searcher, &AllQuery, small, vec![1.0f32, 0.0], 5)?;
    assert_eq!(hits.len(), 5);
    assert_descending(&hits);
    assert!(
        (score_at(&hits, 0) - 1.0).abs() < 1e-5,
        "top: {:?}",
        hits[0].0
    );
    let last = hits.last().unwrap().0;
    assert!((last + 1.0).abs() < 1e-5);

    // The big field is independently queryable on the same docs.
    let hits = search_top_n(&searcher, &AllQuery, big, vec![0.0f32, 0.0, 0.0, 1.0], 3)?;
    assert_descending(&hits);
    assert!((score_at(&hits, 0) - 1.0).abs() < 1e-5);

    // Filter: group == 2 (one orthogonal + one opposite to query in `small`).
    let group2 = RangeQuery::new(
        Bound::Included(Term::from_field_u64(group, 2)),
        Bound::Included(Term::from_field_u64(group, 2)),
    );
    let hits = search_top_n(&searcher, &group2, small, vec![1.0f32, 0.0], 10)?;
    assert_eq!(hits.len(), 2);
    assert_descending(&hits);
    assert!(
        score_at(&hits, 0).abs() < 1e-5,
        "orthogonal first: {:?}",
        hits[0].0
    );
    assert!((score_at(&hits, 1) + 1.0).abs() < 1e-5);

    Ok(())
}

// ---- Dot: bool filter ----

#[test]
fn test_search_dot() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let active = schema_builder.add_bool_field("active", INDEXED | STORED);
    let embed = schema_builder.add_vector_field("embedding", VectorOptions::new(3, Metric::Dot));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    // (active, vector) — five docs split across two segments. Dot is
    // magnitude-sensitive: [2,0,0]·[1,0,0] = 2 beats [1,0,0]·[1,0,0] = 1.
    let docs = [
        (true, [1.0f32, 0.0, 0.0]),
        (true, [2.0, 0.0, 0.0]),
        (false, [1.0, 0.0, 0.0]),
        (false, [-1.0, 0.0, 0.0]),
        (true, [0.0, 1.0, 0.0]),
    ];
    for (i, (a, v)) in docs.iter().enumerate() {
        let mut doc = TantivyDocument::new();
        doc.add_bool(active, *a);
        doc.add_vector(embed, v);
        writer.add_document(doc)?;
        if i == 2 {
            writer.commit()?;
        }
    }
    writer.commit()?;

    let searcher = index.reader()?.searcher();
    let query_vec = vec![1.0f32, 0.0, 0.0];

    // Pure: dot products are 2, 1, 1, 0, -1.
    let hits = search_top_n(&searcher, &AllQuery, embed, query_vec.clone(), 5)?;
    assert_eq!(hits.len(), 5);
    assert_descending(&hits);
    assert!(
        (score_at(&hits, 0) - 2.0).abs() < 1e-5,
        "top: {:?}",
        hits[0].0
    );
    let last = hits.last().unwrap().0;
    assert!((last + 1.0).abs() < 1e-5);

    // Filtered: only active=true (3 docs: dot=2, dot=1, dot=0).
    let active_true = TermQuery::new(
        Term::from_field_bool(active, true),
        IndexRecordOption::Basic,
    );
    let hits = search_top_n(&searcher, &active_true, embed, query_vec, 10)?;
    assert_eq!(hits.len(), 3);
    assert_descending(&hits);
    assert!((score_at(&hits, 0) - 2.0).abs() < 1e-5);
    assert!(score_at(&hits, 2).abs() < 1e-5);

    Ok(())
}

// ---- Sparse: not every doc has a vector ----

#[test]
fn test_search_sparse_vectors() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | STORED);
    let embed = schema_builder.add_vector_field("embedding", VectorOptions::new(3, Metric::L2));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    // Six docs, only some have a vector.
    // (category, optional vector)
    let docs: &[(&str, Option<[f32; 3]>)] = &[
        ("alpha", Some([1.0, 0.0, 0.0])),
        ("beta", None), // no embedding
        ("alpha", Some([0.9, 0.1, 0.0])),
        ("beta", None), // no embedding
        ("alpha", Some([-1.0, 0.0, 0.0])),
        ("gamma", None), // no embedding
    ];
    for (cat, v) in docs {
        let mut doc = TantivyDocument::new();
        doc.add_text(category, cat);
        if let Some(v) = v {
            doc.add_vector(embed, v);
        }
        writer.add_document(doc)?;
    }
    writer.commit()?;

    let searcher = index.reader()?.searcher();
    let segments = searcher.segment_readers();
    assert_eq!(segments.len(), 1);

    // Plugin reader: column reports only 3 present docs out of 6.
    let vec_reader = crate::vector::VectorReader::open(&segments[0])?;
    let column = vec_reader.open_column(embed)?;
    assert_eq!(column.len(), 3);
    // Present docs: 0, 2, 4
    for d in [0u32, 2, 4] {
        assert!(column.contains(d), "doc {d} should be present");
        assert!(column.vector_bytes_at(d).is_some());
    }
    // Absent docs: 1, 3, 5 — bitmap says no, lookup returns None
    for d in [1u32, 3, 5] {
        assert!(!column.contains(d), "doc {d} should be absent");
        assert!(column.vector_bytes_at(d).is_none());
    }

    // Pure vector search: only the 3 docs with vectors come back —
    // vectorless docs are dropped (TopDocsByVectorSimilarity contract,
    // imposed by IVF compatibility).
    let hits = search_top_n(&searcher, &AllQuery, embed, vec![1.0f32, 0.0, 0.0], 10)?;
    assert_eq!(hits.len(), 3, "only docs with vectors come back");
    assert_descending(&hits);
    // Top hit: exact match -> -l2² == 0
    assert!(score_at(&hits, 0).abs() < 1e-6);

    // Filtered + sparse: filter to "beta" (which has no vectors).
    // Empty result — beta docs match the filter but have no vectors.
    let beta = TermQuery::new(
        Term::from_field_text(category, "beta"),
        IndexRecordOption::Basic,
    );
    let hits = search_top_n(&searcher, &beta, embed, vec![1.0f32, 0.0, 0.0], 10)?;
    assert!(
        hits.is_empty(),
        "beta docs have no vectors → empty result, got {hits:?}",
    );

    Ok(())
}

// ---- Merge with sparse vectors ----

#[test]
fn test_merge_preserves_sparsity() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | STORED);
    let embed = schema_builder.add_vector_field("embedding", VectorOptions::new(2, Metric::L2));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    // Segment 1: 2 docs, only the second has a vector.
    let mut doc = TantivyDocument::new();
    doc.add_text(category, "no_vec_1");
    writer.add_document(doc)?;
    let mut doc = TantivyDocument::new();
    doc.add_text(category, "has_vec_1");
    doc.add_vector(embed, &[1.0f32, 0.0]);
    writer.add_document(doc)?;
    writer.commit()?;

    // Segment 2: 2 docs, only the first has a vector.
    let mut doc = TantivyDocument::new();
    doc.add_text(category, "has_vec_2");
    doc.add_vector(embed, &[0.0f32, 1.0]);
    writer.add_document(doc)?;
    let mut doc = TantivyDocument::new();
    doc.add_text(category, "no_vec_2");
    writer.add_document(doc)?;
    writer.commit()?;

    // Merge.
    let segment_ids: Vec<SegmentId> = index.searchable_segment_ids()?.into_iter().collect();
    assert_eq!(segment_ids.len(), 2);
    writer.merge(&segment_ids).wait()?;
    writer.wait_merging_threads()?;

    // After merge: 4 docs in one segment, exactly 2 have vectors.
    let searcher = index.reader()?.searcher();
    let segments = searcher.segment_readers();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_doc(), 4);
    let vec_reader = crate::vector::VectorReader::open(&segments[0])?;
    let column = vec_reader.open_column(embed)?;
    assert_eq!(column.len(), 2, "two vectors should survive the merge");
    let present: Vec<u32> = (0..4).filter(|&d| column.contains(d)).collect();
    assert_eq!(present.len(), 2);
    Ok(())
}

// ---- Merge ----

#[test]
fn test_flat_vec_plugin_merge() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT | STORED);
    let vec_field = schema_builder.add_vector_field("embedding", VectorOptions::new(4, Metric::L2));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    let mut doc = TantivyDocument::new();
    doc.add_text(text_field, "a");
    doc.add_vector(vec_field, &[1.0, 2.0, 3.0, 4.0]);
    writer.add_document(doc)?;
    writer.commit()?;

    let mut doc = TantivyDocument::new();
    doc.add_text(text_field, "b");
    doc.add_vector(vec_field, &[5.0, 6.0, 7.0, 8.0]);
    writer.add_document(doc)?;

    let mut doc = TantivyDocument::new();
    doc.add_text(text_field, "c");
    doc.add_vector(vec_field, &[9.0, 10.0, 11.0, 12.0]);
    writer.add_document(doc)?;
    writer.commit()?;

    let segment_ids: Vec<SegmentId> = index.searchable_segment_ids()?.into_iter().collect();
    assert_eq!(segment_ids.len(), 2);
    writer.merge(&segment_ids).wait()?;
    writer.wait_merging_threads()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let segments = searcher.segment_readers();
    assert_eq!(segments.len(), 1);

    let vec_reader = crate::vector::VectorReader::open(&segments[0])?;
    let column = vec_reader.open_column(vec_field)?;

    let decode = |bytes: &[u8]| -> Vec<f32> {
        bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect()
    };

    let mut got: Vec<Vec<f32>> = (0..segments[0].max_doc())
        .filter_map(|d| column.vector_bytes_at(d).map(decode))
        .collect();
    got.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
    assert_eq!(
        got,
        vec![
            vec![1.0, 2.0, 3.0, 4.0],
            vec![5.0, 6.0, 7.0, 8.0],
            vec![9.0, 10.0, 11.0, 12.0],
        ]
    );
    Ok(())
}
