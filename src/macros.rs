
macro_rules! get(
    ($e:expr) => (match $e { Some(e) => e, None => return None })
);


/// `doc!` is a shortcut that helps building `Document`
/// object, assuming you have the field object.
///
/// # Example
///
/// ```
/// #[macro_use]
/// extern crate tantivy;
/// 
/// use tantivy::schema::{SchemaBuilder, TEXT, FAST};
/// 
/// //...
///
/// # fn main() {
/// let mut schema_builder = SchemaBuilder::new();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let author = schema_builder.add_text_field("text", TEXT);
/// let likes = schema_builder.add_u64_field("num_u64", FAST); 
/// let schema = schema_builder.build();
/// let doc = doc!(
/// 	title => "Life Aquatic",
/// 	author => "Wes Anderson",
/// 	likes => 4u64
/// );
/// # }
/// ```
#[macro_export]
macro_rules! doc(
    () => {
        {
            ($crate::Document::default())
        }
    }; // avoids a warning due to the useless `mut`.
    ($($field:ident => $value:expr),*) => {
        {
            let mut document = $crate::Document::default();
            $(
                document.add($crate::schema::FieldValue::new($field, $value.into()));
            )*
            document
        }
    };
);