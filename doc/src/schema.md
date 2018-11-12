# Schema

When starting a new project using tantivy, your first step will be to your schema. Be aware that changing it will probably require you to reindex all of your data.
It is strongly recommended you keep the means to iterate through your original data when this happens.

If not specified otherwise, tantivy does not keep a raw version of your data,
so the good practise is to rely on a distinct storage to store your
raw documents.

The schema defines both the type of the fields you are indexing, but also the type of indexing you want to apply to them. The set of search operations that you will be able to perform depends on the way you set up your schema.

Here is what defining your schema could look like.

```Rust
use tantivy::schema::{Schema, TEXT, STORED, INT_INDEXED};

let mut schema_builder = SchemaBuilder::default();
let text_field = schema_builder.add_text_field("name", TEXT | STORED);
let tag_field = schema_builder.add_facet_field("tags");
let timestamp_field = schema_buider.add_u64_field("timestamp", INT_INDEXED)
let schema = schema_builder.build();
```

Notice how adding a new field to your schema builder
follows the following pattern :

```verbatim
  schema_builder.add_<fieldtype>_field("<fieldname>", <field_configuration>);
```

This method returns a `Field` handle that will be used for all kind of 

# Field types

Tantivy currently supports only 4 types.

- `text` (understand `&str`)
- `u64` and `i64`
- `HierarchicalFacet`

Let's go into their specificities.

# Text

Full-text search is the bread and butter of search engine.
The key idea is fairly simple. Your text is broken apart into tokens (that's
what we call tokenization). Tantivy then keeps track of the list of the documents containing each token.

In order to increase recall you might want to normalize tokens. For instance,
you most likely want to lowercase your tokens so that documents match the query `cat` regardless of whether your they contain the token `cat` or `Cat`.
