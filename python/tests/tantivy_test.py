import json
import tantivy


class TestClass(object):
    def test_simple_search(self):
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

        doc = schema.parse_document(json.dumps({
            "title": "Of Mice and Men",
            "body": ("A few miles south of Soledad, the Salinas River drops "
                     "in close to the hillside bank and runs deep and "
                     "green. The water is warm too, for it has slipped "
                     "twinkling over the yellow sands in the sunlight "
                     "before reaching the narrow pool. On one side of the "
                     "river the golden foothill slopes curve up to the "
                     "strong and rocky Gabilan Mountains, but on the valley "
                     "side the water is lined with trees—willows fresh and "
                     "green with every spring, carrying in their lower leaf "
                     "junctures the debris of the winter’s flooding; and "
                     "sycamores with mottled, white, recumbent limbs and "
                     "branches that arch over the pool")
        }))

        writer.add_document(doc)

        doc = schema.parse_document(json.dumps({
            "title": ["Frankenstein", "The Modern Prometheus"],
            "body": ("You will rejoice to hear that no disaster has "
                     "accompanied the commencement of an enterprise which you "
                     "have regarded with such evil forebodings.  I arrived "
                     "here yesterday, and my first task is to assure my dear "
                     "sister of my welfare and increasing confidence in the "
                     "success of my undertaking.")
        }))

        writer.add_document(doc)
        writer.commit()

        reader = index.reader()
        searcher = reader.searcher()

        query_parser = tantivy.QueryParser.for_index(index, [title, body])
        query = query_parser.parse_query("sea whale")

        top_docs = tantivy.TopDocs(10)

        result = searcher.search(query, top_docs)
        print(result)

        assert len(result) == 1

        _, doc_address = result[0]

        searched_doc = searcher.doc(doc_address)
        assert searched_doc.get_first(title) == "The Old Man and the Sea"

    def test_doc(self):
        builder = tantivy.SchemaBuilder()
        title = builder.add_text_field("title", stored=True)

        doc = tantivy.Document()
        assert doc.is_empty

        doc.add_text(title, "The Old Man and the Sea")

        assert doc.get_first(title) == "The Old Man and the Sea"

        assert doc.len == 1
        assert not doc.is_empty
