python-tantivy
==============

Python bindings for tantivy.


# Installation

The bindings can be installed using setuptools:

    python setup.py install --user

Note that this requires setuptools-rust to be installed. Another thing to note
is that the bindings are using [PyO3](https://github.com/PyO3/pyo3), which
requires rust nightly currently.

# Usage

python-tantivy has a similar API to tantivy. To create a index first a schema
needs to be built. After that documents can be added to the index and a reader
can be created to search the index.

```python
    builder = tantivy.SchemaBuilder()

    title = builder.add_text_field("title", stored=True)
    body = builder.add_text_field("body")

    schema = builder.build()
    index = tantivy.Index(schema)

    writer = index.writer()

    doc = tantivy.Document()
    doc.add_text(title, "The Old Man and the Sea")
    doc.add_text(body, ("He was an old man who fished alone in a skiff in"
                        "the Gulf Stream and he had gone eighty-four days "
                        "now without taking a fish."))
    writer.add_document(doc)

    reader = index.reader()
    searcher = reader.searcher()

    query_parser = tantivy.QueryParser.for_index(index, [title, body])
    query = query_parser.parse_query("sea whale")

    top_docs = tantivy.TopDocs(10)
    result = searcher.search(query, top_docs)

    _, doc_address = result[0]

    searched_doc = searcher.doc(doc_address)
    assert searched_doc.get_first(title) == "The Old Man and the Sea"
```
