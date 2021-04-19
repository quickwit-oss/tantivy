# Tantivy index file formats
This document defines the index file formats used in Tantivy. 

## Definitions
### Index and segments
Tantivy indexes is composed of multiple sub-indexes called segments.


## Index metadata json
For a given index, Tantivy stores the following metadata in a json file `meta.json` :
- the list of segments with its metadata id, max_doc, deletes ;
- the index schema represented by the list of fields with name, type and option data ;
- the opstamp or timestamp of the operation (commit).

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
| Posting list | `.idx` | Sorted lists of document ids, associated to terms |
| Term dictionnary | `.term` | Dictionary associating `Term`s to an address into the `postings` file and the `positions` file |
| Term positions | `.pos` | Positions of terms in each document |
| Position file index | `.posidx` | Index to seek within the position file |
| Document store | `.store` | Row-oriented, compressed storage of the documents |
| Fast fields | `.fast` | Column-oriented random-access storage of fields |
| Fieldnorm | `.fieldnorm` | Stores the sum  of the length (in terms) of each field for each document ? |
| Tombstone | `.del` | Bitset describing which document of the segment is  |

### Posting list `.idx`

**Questions**
- what happens if there are no position to store ? Does the file exist ?


### Term dictionnary `.term`

### Token positions in documents `.pos` and `.posidx`
Token positions are stored in three parts and over two files:
- File `.pos`: contains bitpacked of the positions delta
- File `.posidx`: contains bytes and long skip index


#### Position file `.pos`
The position file contains bitpacked positions delta (the positions are already delta encoded before writing) for all terms of a given field, one term after the other. 
Positions are bitpacked by block of size 1024, each position is stored as an **u32**.


#### File `.posidx`
This file is organized in 3 parts:
- Part 1: list of number of bytes used to decompress bitpacked blocks. Number of bytes is stored as an **usize**. There is one usize per compressed block.
- Part 2: list of offsets for every 1024 compression blocks. An offset is stored as an **u64**.  
- Footer: number of long splits stored as an **u32**  


### Doc store `.pos`

### Fast fields `.pos`

### Fieldnorm `.pos`

### Delete documents `.del`



