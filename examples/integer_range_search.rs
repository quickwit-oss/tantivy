use std::ops::Bound;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, RangeQuery, TermQuery};
use tantivy::schema::{self, BytesOptions, Schema, SchemaBuilder, Value, FAST, INDEXED, STORED};
use tantivy::{Document, Index, Result, Searcher, TantivyDocument, Term};

// Define the data structure for lines
#[derive(Debug)]
struct Line {
    from: u64,
    to: u64,
    interpolation: u64, // 0(even),1(odd),2(all),
    coordinates: Vec<(f64, f64)>,
}

// Function to serialize coordinates into a binary format
fn serialize_coordinates(coords: &[(f64, f64)]) -> Vec<u8> {
    let mut buffer = Vec::new();
    for &(lon, lat) in coords {
        buffer.extend_from_slice(&lon.to_le_bytes());
        buffer.extend_from_slice(&lat.to_le_bytes());
    }
    buffer
}

// Function to deserialize coordinates from the binary format
fn deserialize_coordinates(data: &[u8]) -> Vec<(f64, f64)> {
    let mut coords = Vec::new();
    let mut cursor = data;
    while cursor.len() >= 16 {
        let lon = f64::from_le_bytes(cursor[0..8].try_into().unwrap());
        let lat = f64::from_le_bytes(cursor[8..16].try_into().unwrap());
        coords.push((lon, lat));
        cursor = &cursor[16..];
    }
    coords
}

// Function to search and retrieve coordinates from the index
fn retrieve_coordinates(
    searcher: &Searcher,
    retrieved_doc: &TantivyDocument,
) -> Option<Vec<(f64, f64)>> {
    let coordinates_binary =
        retrieved_doc.get_first(searcher.schema().get_field("coordinates").unwrap())?;
    Some(deserialize_coordinates(coordinates_binary.as_bytes()?))
}

// Function to perform interpolation on the coordinates
fn interpolate(
    coordinates: &[(f64, f64)],
    from: u64,
    to: u64,
    house_number: u64,
) -> Option<(f64, f64)> {
    if from == to {
        // Return the center of coordinates if no range
        let (sum_lon, sum_lat) = coordinates
            .iter()
            .fold((0.0, 0.0), |(sum_lon, sum_lat), &(lon, lat)| {
                (sum_lon + lon, sum_lat + lat)
            });
        let count = coordinates.len() as f64;
        return Some((sum_lon / count, sum_lat / count));
    }

    let ratio = (house_number - from) as f64 / (to - from) as f64;
    let total_distance: f64 = coordinates.windows(2).map(|w| dist(w[0], w[1])).sum();
    let target_distance = ratio * total_distance;

    let mut accumulated_distance = 0.0;
    for pair in coordinates.windows(2) {
        let segment_distance = dist(pair[0], pair[1]);
        if accumulated_distance + segment_distance >= target_distance {
            let remaining = target_distance - accumulated_distance;
            let segment_ratio = remaining / segment_distance;
            let (lon1, lat1) = pair[0];
            let (lon2, lat2) = pair[1];
            let lon = lon1 + (lon2 - lon1) * segment_ratio;
            let lat = lat1 + (lat2 - lat1) * segment_ratio;
            return Some((lon, lat));
        }
        accumulated_distance += segment_distance;
    }

    None
}

// Function to calculate the Euclidean distance between two points
fn dist(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    ((p1.0 - p2.0).powi(2) + (p1.1 - p2.1).powi(2)).sqrt()
}

// Build the Tantivy schema
fn build_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::new();
    let _ = schema_builder.add_u64_field("from", FAST | STORED | INDEXED);
    let _ = schema_builder.add_u64_field("to", FAST | STORED | INDEXED);
    let _ = schema_builder.add_u64_field("interpolation", FAST | STORED | INDEXED);
    let _ = schema_builder.add_bytes_field(
        "coordinates",
        BytesOptions::default()
            .set_fast()
            .set_indexed()
            .set_stored(),
    );
    schema_builder.build()
}

fn create_index_and_add_data(lines: Vec<Line>) -> Result<Index> {
    let schema = build_schema();
    let index = Index::create_in_ram(schema.clone());
    let mut writer = index.writer(50_000_000)?;

    for line in lines {
        let serialized_coords = serialize_coordinates(&line.coordinates);
        let mut doc = TantivyDocument::default();
        doc.add_u64(schema.get_field("from")?, line.from);
        doc.add_u64(schema.get_field("to")?, line.to);
        doc.add_u64(schema.get_field("interpolation")?, line.interpolation);
        doc.add_bytes(schema.get_field("coordinates")?, &serialized_coords);
        writer.add_document(doc)?;
    }

    writer.commit()?;
    Ok(index)
}

fn main() -> Result<()> {
    // Sample data
    let lines = vec![
        Line {
            from: 100,
            to: 108,
            interpolation: 0,
            coordinates: vec![
                (32.438403, -86.649160),
                (32.438380, -86.649391),
                (32.438329, -86.649988),
                (32.438316, -86.650243),
                (32.438311, -86.650408),
                (32.438310, -86.650412),
            ],
        },
        Line {
            from: 1,
            to: 107,
            interpolation: 1,
            coordinates: vec![
                (32.438239, -86.649136),
                (32.438214, -86.649369),
                (32.438163, -86.649972),
                (32.438148, -86.650233),
                (32.438145, -86.650410),
                (32.438146, -86.650421),
            ],
        },
    ];

    // Create index and add data
    let index = create_index_and_add_data(lines)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let search_hnr = 106;
    let from_query = RangeQuery::new(
        Bound::Unbounded,
        Bound::Included(Term::from_field_u64(
            index.schema().get_field("from")?,
            search_hnr,
        )),
        None,
        None,
        None
    );
    let to_query = RangeQuery::new(
        Bound::Included(Term::from_field_u64(
            index.schema().get_field("to")?,
            search_hnr,
        )),
        Bound::Unbounded,
        None,
        None,
        None
    );

    let interpolate_field = index.schema().get_field("interpolation")?;
    let what_term_query = TermQuery::new(
        Term::from_field_u64(interpolate_field, search_hnr % 2),
        schema::IndexRecordOption::Basic,
    );
    let all_term_query = TermQuery::new(
        Term::from_field_u64(interpolate_field, 2),
        schema::IndexRecordOption::Basic,
    );

    let interpolate_query = BooleanQuery::union_with_minimum_required_clauses(
        vec![Box::new(what_term_query), Box::new(all_term_query)],
        1,
    );

    let query = BooleanQuery::intersection(vec![
        Box::new(interpolate_query),
        Box::new(from_query),
        Box::new(to_query),
    ]);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    for (_score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
        // Retrieve data from the index and perform interpolation
        let coordinates = retrieve_coordinates(&searcher, &retrieved_doc).unwrap();
        let from = retrieved_doc
            .get_first(index.schema().get_field("from")?)
            .unwrap()
            .as_u64()
            .unwrap();
        let to = retrieved_doc
            .get_first(index.schema().get_field("to")?)
            .unwrap()
            .as_u64()
            .unwrap();
        let interpolated_coord = interpolate(&coordinates, from, to, search_hnr);
        println!(
            "Score {} - HNR {} - Doc {} - Coord {:?}",
            _score,
            search_hnr,
            retrieved_doc.to_json(&index.schema()),
            interpolated_coord
        );
    }

    Ok(())
}
