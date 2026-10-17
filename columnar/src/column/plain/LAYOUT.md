# Plain column layout

`str` and `bytes` columns have one of two codecs. The codec is part of the configuration: it is
never detected from the data.

- **Dictionary** (default): a sorted term dictionary (sstable) and a column of term ordinals.
- **Plain**: the values themselves, without term ordinals. Better suited to columns with a
  high cardinality.

This file describes the plain codec.

## Column key and columnar version

The last byte of the column key is `column_type` for dictionary encoded and non-text columns,
and `0x10 | column_type` for plain-encoded columns (`0x13` for bytes, `0x14` for str).
Column keys must be inserted in sorted order, so a plain column is serialized after the other
columns with the same name.

The columnar footer has version 3 if, and only if, it contains at least one plain column.
Columnars without plain columns keep version 2, and older readers can still open them.

## Plain column payload

All integers are little endian.

```
PLAIN_COLUMN :=
    [BLOCK]*                                # read lazily, one block at a time
    [BLOCK_INDEX]
    [COLUMN_INDEX]                          # same format as for the other columns
    [BLOCK_INDEX_NUM_BYTES: u32]
    [COLUMN_INDEX_NUM_BYTES: u32]
    [PLAIN_FORMAT_VERSION: u8 = 1]

BLOCK_INDEX :=
    [NUM_BLOCKS: u32]
    [VALUE_STARTS: u32 * (NUM_BLOCKS + 1)]  # first value idx of each block, then num values
    [BYTE_STARTS: u64 * (NUM_BLOCKS + 1)]   # start of each block, then the blocks length
```

A block holds at most 100,000 values, and at most 10 MiB of raw bytes, unless it holds a single
larger value. Each block is self-contained: reading a value only requires reading its block.

```
ONPAIR16_BLOCK :=
    [KIND: u8 = 1]
    [NUM_TOKENS - 1: u16]
    [DICT_BYTES_LEN: u32]
    [CODE_OFFSETS_LEN: u32]
    [NUM_CODES: u32]
    [TOKEN_LENS: 4 bits * NUM_TOKENS]       # token length - 1, token 2k in the low nibble
    [DICT_BYTES]                            # tokens concatenated, with the onpair read padding
    [CODE_OFFSETS]                          # u64 based column values, NUM_VALUES + 1 entries
    [CODES: ceil(NUM_CODES * CODE_BITS / 8) + 7]  # CODE_BITS bits per code, then 7 zero bytes

CODE_BITS := ceil(log2(NUM_TOKENS))         # not stored: the minimal width of a token id

RAW_BLOCK :=                                # for small or incompressible blocks
    [KIND: u8 = 0]
    [OFFSETS_LEN: u32]
    [OFFSETS]                               # u64 based column values, NUM_VALUES + 1 entries
    [VALUE_BYTES]
```

Reading a value:
- doc -> value idx: the column index.
- value idx -> block: binary search in `VALUE_STARTS`.
- value idx -> codes: `CODES[CODE_OFFSETS[slot] .. CODE_OFFSETS[slot + 1]]`, with
  `slot = value idx - VALUE_STARTS[block]`.
- codes -> bytes: OnPair decoding with the dictionary of the block.
