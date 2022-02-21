# Json

As of tantivy 0.17, tantivy supports a json object type.
This type can be used to allow for a schema-less search index.

When indexing a json object, we "flatten" the JSON. This operation emits terms that represent a triplet `(json_path, value_type, value)`

For instance,  if user is a json field, the following document:

```json
{
    "user": {
        "name": "Paul Masurel",
        "address": {
            "city": "Tokyo",
            "country": "Japan"
        },
        "created_at": "2018-11-12T23:20:50.52Z"
    }
}
```

emits the following tokens:
-  ("name", Text, "Paul")
-  ("name", Text, "Masurel")
-  ("address.city", Text, "Tokyo")
-  ("address.country", Text, "Japan")
-  ("created_at", Date, 15420648505)


# Bytes-encoding and lexicographical sort.

Like any other terms, these triplets are encoded into a binary format as follows.
- `json_path`: the json path is a sequence of "segments". In the example above, `address.city`
is just a debug representation of the json path `["address", "city"]`.
Its representation is done by separating segments by a unicode char `\x01`, and ending the path by `\x00`.
- `value type`: One byte represents the `Value` type.
- `value`: The value representation is just the regular Value representation.

This representation is designed to align the natural sort of Terms with the lexicographical sort
of their binary representation (Tantivy's dictionary (whether fst or sstable) is sorted and does prefix encoding).

In the example above, the terms will be sorted as
-  ("address.city", Text, "Tokyo")
-  ("address.country", Text, "Japan")
-  ("name", Text, "Masurel")
-  ("name", Text, "Paul")
-  ("created_at", Date, 15420648505)

As seen in "pitfalls", we may end up having to search for a value for a same path in several different fields. Putting the field code after the path makes it maximizes compression opportunities but also increases the chances for the two terms to end up in the actual same term dictionary block.


# Pitfalls and limitation.

Json gives very little information about the type of the literals it stores.
All numeric types end up mapped as a "Number" and there are no types for dates.

At ingestion time, tantivy will try to interpret number and strings as different type with a
priority order.
Numbers will be interpreted as u64, i64 and f64 in that order.
Strings will be interpreted as rfc3999 dates or simple strings.

The first working time is picked and only one type will be emitted for indexing.

Note this interpretation happens on a per-document basis, and there is no effort to try to sniff
a consistent field type at the scale of a segment.

On the query parser side on the other hand, we may end up emitting more than one type.
For instance, we do not even know if the type is a number or string based.

So the query

```
my_path.my_segment:233
```

Should be interpreted as
- `(my_path.my_segment, String, 233)`
- `(my_path.my_segment, u64, 233)`

Likewise, we need to emit two tokens if the query contains an rfc3999 date.
Indeed the date could have been actually a single token inside the text of a document at ingestion time. Generally speaking, we will always at least emit a string token in query parsing, and sometimes more.
