# Tantivy index file formats
This document defines the index file formats used in Tantivy. 

## Definitions
### Index and segments
Tantivy indexes is composed of multiple sub-indexes called segments.


## Index metadata json
For a given index, Tantivy stores the following metadata in a json file `meta.json` :
- the list of segments with its metadata id, max_doc, deletes ;
- the index schema represented by the list of fields with name, type and option data ;
- the opstamp

```
{
  "segments": [
    {
      "segment_id": uuid string,
      "max_doc": int|null,
      "deletes": int|null
    },
    ...
  ],
  "schema": [
    {
      "name": string,
      "type": string,
      "options": {
        "indexing": {
          "record": string,
          "tokenizer": string
        },
        "stored": bool
      }
    },
 ],
 ...
 "opstamp": int
}
```


## Summary of segment file extensions
For a given segment, Tantivy stores a bunch of files whose name is set by segment uuid and whose extension defines the datastructure.

| Name | Extension | Description |
| --- | --- | --- |
| Doc ids | `.idx` | Id |
| Term dictionnary | `.term` | Terms |
| Token positions | `.pos` | Token positions |
| Token positions skip | `.posidx` | Token positions skip |
| Docstore | `.store` | Docstore |
| Fastfields | `.fast` | Fast fields |
| Fieldnorm | `.fieldnorm` | Fieldnorm (BM25) |
| Tombstone | `.del` | Deleted docs id |

### Doc ids `.idx`

### Term dictionnary `.term`

### Token positions `.pos`

### Token positions skip `.pos`

### Doc store `.pos`

### Fast fields `.pos`

### Fieldnorm `.pos`

### Delete documents `.del`



