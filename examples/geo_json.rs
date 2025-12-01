use tantivy::collector::TopDocs;
use tantivy::query::SpatialQuery;
use tantivy::schema::{Schema, Value, SPATIAL, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    schema_builder.add_json_field("properties", STORED | TEXT);
    schema_builder.add_spatial_field("geometry", STORED | SPATIAL);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
            "type":"Feature",
            "geometry":{
                "type":"Polygon",
                "coordinates":[[[-99.483911,45.577697],[-99.483869,45.571457],[-99.481739,45.571461],[-99.474881,45.571584],[-99.473167,45.571615],[-99.463394,45.57168],[-99.463391,45.57883],[-99.463368,45.586076],[-99.48177,45.585926],[-99.48384,45.585953],[-99.483885,45.57873],[-99.483911,45.577697]]]
            },
            "properties":{
                "admin_level":"8",
                "border_type":"city",
                "boundary":"administrative",
                "gnis:feature_id":"1267426",
                "name":"Hosmer",
                "place":"city",
                "source":"TIGER/Line® 2008 Place Shapefiles (http://www.census.gov/geo/www/tiger/)",
                "wikidata":"Q2442118",
                "wikipedia":"en:Hosmer, South Dakota"
            }
        }"#,
    )?;
    index_writer.add_document(doc)?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let field = schema.get_field("geometry").unwrap();
    let query = SpatialQuery::new(
        field,
        [(-99.49, 45.56), (-99.45, 45.59)],
        tantivy::query::SpatialQueryType::Intersects,
    );
    let hits = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
    for (_score, doc_address) in &hits {
        let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
        if let Some(field_value) = retrieved_doc.get_first(field) {
            if let Some(geometry_box) = field_value.as_value().into_geometry() {
                println!("Retrieved geometry: {:?}", geometry_box);
            }
        }
    }
    assert_eq!(hits.len(), 1);
    Ok(())
}
