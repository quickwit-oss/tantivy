# Tantivy index file formats
This document defines the index file formats used in Tantivy. 

Tantivy has one metadata json file for each index and a few files for each segment, each file storing a specific datastructure.

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


## Endianness
By default integers and floats are serialized with little indian order. In some specific cases, Tantivy uses big indian, the documentation will explicitely indicates it in this case.


## Summary of segment file extensions
For a given segment, Tantivy stores a bunch of files whose name is set by segment uuid and whose extension defines the datastructure.

| Name | Extension | Description |
| --- | --- | --- |
| Posting list | `.idx` | Sorted lists of document ids, associated to terms |
| Term dictionnary | `.term` | Dictionary associating `Term`s to an address into the `postings` file and the `positions` file |
| Term positions | `.pos` | Positions of terms in each document |
| Term positions file index | `.posidx` | Index to seek within the position file |
| Document store | `.store` | Row-oriented, compressed storage of the documents |
| Fast fields | `.fast` | Column-oriented random-access storage of fields |
| Fieldnorm | `.fieldnorm` | Stores the sum  of the length (in terms) of each field for each document ? |
| Tombstone | `.del` | Bitset describing which document of the segment is deleted  |


### Composite file structure
[Source](../../src/common/composite_file.rs)

All segment files needs to store data for each field except for tomstone file (.del). In this case, a footer is added which stores for each field an offset that indicates the starting point (or file address) of its data.

Footer-->{{offset, file_address}<sup>num_field</sup>, num_field, footer_len}
offset--> VInt
file_address-->{field_id, idx}
field_id-->u32
idx-->VInt
num_field-->VInt
footer_len-->u32


### Posting list `.idx`
[Ref](../../src/postings/serializer.rs)

Posting list file is a composite file so it contains the footer data as described in composite file section.

The posting list is composed of blocks of 128 documents where the following data is stored:
- last doc id encoded stored as **u32**
- number of bytes used to decompress bitpacked doc ids stored as **u8**
- if field has frequency: 
  - number of bytes used to decompress bitpacked term frequencies stored as **u8**
  - fieldnorm_id as **u8**
  - block_wand_max as **u8**
- delta encoded and bitpacked doc ids
- bitpacked term frequencies if field has frequency


### Term dictionnary `.term`

### Token positions in documents `.pos` and `.posidx`
[Ref](../../src/positions/serializer.rs)


Token positions are stored in three parts and over two files:
- File `.pos`: contains bitpacked of the positions delta
- File `.posidx`: contains bytes and long skip index


#### Position file `.pos`
The position file contains bitpacked positions delta (the positions are already delta encoded before writing) for all terms of a given field, one term after the other. 
Positions are bitpacked by block of size 1024, each position is stored as an **u32**.


#### File `.posidx`
This file is organized in 3 parts:
- Part 1: list of number of bytes used to decompress bitpacked blocks, stored as an **u8**. There is one usize per compressed block.
- Part 2: list of offsets for every 1024 compression blocks. An offset is stored as an **u64**.  
- Footer: number of long splits stored as an **u32**  


### Doc store `.pos`

### Fast fields `.pos`

### Fieldnorm `.pos`

### Delete documents `.del`



