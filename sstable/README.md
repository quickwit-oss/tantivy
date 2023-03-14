# SSTable

The `tantivy-sstable` crate is yet another sstable crate.

It has been designed to be used in `quickwit`:
- as an alternative to the default tantivy fst dictionary.
- as a way to store the column index for dynamic fast fields.

The benefit compared to the fst crate is locality.
Searching a key in the fst crate requires downloading the entire dictionary.

Once the sstable index is downloaded, running a `get` in the sstable
crate only requires a single fetch.

Right now, the block index and the default block size have been thought
for quickwit, and the performance of a get is very bad.

# Sorted strings?

SSTable stands for Sorted String Table.
Strings have to be insert in sorted order.

That sorted order is used in different ways:
- it makes gets and streaming ranges of keys
possible.
- it allows incremental encoding of the keys
- the front compression is leveraged to optimize
the intersection with an automaton

# On disk format

The on disk format is made to allow for partial loading from a sparse FileSlice. A single SSTable is composed of a sequence of independant blocks followed by an index.

This definition uses a rust like syntax. Numbers are encoded in little endian unless noted otherwise.
```rust
struct SSTable {
    /// terminated by an empty last block
    blocks: [Block],
    index: Index,
}

struct Block {
    /// byte_len(values) + byte_len(deltas)
    block_len: u32,
    /// user defined format, represent a sequence of values, in the same order as the sequence of key encoded in deltas.
    /// this format must be able to extract its own lenght.
    values: [u8],
    deltas: [Delta]
}

struct Delta {
    keep_add: KeepAdd,
    /// keep_add.add
    suffix: [u8],
}

union KeepAdd {
    /// when both `keep < 16` and `add < 16`
    Small {
        add: u4,
        keep: u4,
    },
    /// otherwise
    Large {
        /// 0x01
        _vint_mode_marker: u8,
        keep: VInt,
        add: VInt,
    }
}

union VInt {
    LastByte {
        /// 0b0
        _continue_bit: u1,
        value: u7
    },
    WithContinuation {
        // 0b1
        _continue_bit: u1,
        value: u7,
        continuation: VInt,
    }
}

impl VInt {
    fn get_u64(self) -> u64 {
        if self._continue_bit == 0 {
            self.value
        } else {
            self.value + self.continuation.get_u64() << 7
        }
    }
}

// TODO this isn't actually implemented. Current format is cbor encoding of the struct
struct Index {
    // this block is a bit unusual as it has no target size like others usually do
    index: Block,
    num_term: u64,
    // TODO insert a version field somewhere along here
    // TODO encode dictionary type
    index_start_offset: u64,
}

/// The format for Block::values for a block inside the Index
struct IndexSSTableValue {
    entry_count: VInt,
    entries: [IndexEntry],
}

struct IndexEntry {
    // last_key_or_greater is encoded inside the key part
    byte_range_start: VInt,
    byte_range_len: VInt,
    first_ordinal: VInt,
}

```
All numbers are little endian.
