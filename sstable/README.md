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

Overview of the SSTable format. Unless noted otherwise, numbers are little-endian.

### SSTable
```
+-------+-------+-----+--------+
| Block | Block | ... | Footer |
+-------+-------+-----+--------+
|----( # of blocks)---|
```
- Block(`SSTBlock`): list of independent block, terminated by a single empty block.
- Footer(`SSTFooter`)

### SSTBlock
```
+----------+----------+--------+-------+-------+-----+
| BlockLen | Compress | Values | Delta | Delta | ... |
+----------+----------+--------+-------+-------+-----+
                      |        |----( # of deltas)---|
                      |------(maybe compressed)------|
```
- BlockLen(u32): length of the block, including the compress byte.
- Compress(u8): indicate whether block is compressed. 0 if not compressed, 1 if compressed.
- Values: an application defined format storing a sequence of value, capable of determining it own length
- Delta

### Delta
```
+---------+--------+
| KeepAdd | Suffix |
+---------+--------+
```
- KeepAdd
- Suffix: KeepAdd.add bytes of key suffix

### KeepAdd
KeepAdd can be represented in two different representation, a very compact 1byte one which is enough for most usage, and a longer variable-len one when required

When keep < 16 and add < 16
```
+-----+------+
| Add | Keep |
+-----+------+
```
- Add(u4): number of bytes to push
- Keep(u4): number of bytes to pop

Otherwise:
```
+------+------+-----+
| 0x01 | Keep | Add |
+------+------+-----+
```
- Add(VInt): number of bytes to push
- Keep(VInt): number of bytes to pop


Note: as the SSTable does not support redundant keys, there is no ambiguity between both representation. Add is always guaranteed to be non-zero, except for the very first key of an SSTable, where Keep is guaranteed to be zero.

### SSTFooter
```
+-------+-------+-----+------------------+------------+-------------+---------+---------+
| Block | Block | ... | FirstLayerOffset | LayerCount | IndexOffset | NumTerm | Version |
+-------+-------+-----+------------------+------------+-------------+---------+---------+
|----(# of blocks)----|---(optional? cf LayerCount)---|
```
- Block(SSTBlock): uses IndexValue for its Values format
- FirstLayerOffset(u64): Offset between the start of the footer and the start of the top level index
- LayerCount(u32): Number of layers of index (min 1) ## TODO do we want to use 0 as a marker for no layers? It makes small sstables 12 bytes more compact (the 0u32 would alias with the "end of sstable marker")
- IndexOffset(u64): Offset to the start of the SSTFooter
- NumTerm(u64): number of terms in the sstable
- Version(u32): Currently equal to 3

Blocks referencing the main table and block referencing the index itself are encoded the same way and
are not directly differentiated. Offsets in blocks referencing the index are relative to the start of
the footer, blocks referencing the main table are relative to the start of that table.

#### TODO(trinity) open questions:
the changes are small enough that it's easy to support both v2 and v3 at once (LayerCount alias with a
4 byte empty block we aways add at the end of an sstable. If LayerCount is zero, we are in v2, and must
not read FirstLayerOffset, if we are in v3, LayerCount is non zero and we read FirstLayerOffset.
If we keep that version number to 2, the format is then also forward compatible: an old version would decode
IndexOffset and after, and find enough information to decode the bottom layer sstable, and would stop at
the empty block added end of that sstable. The non-bottom layers would be loaded to memory, but never actually
processed.
Do we want to support that usage (put back version to v2), or prevent it and use v3?

### IndexValue
```
+------------+----------+-------+-------+-----+
| EntryCount | StartPos | Entry | Entry | ... |
+------------+----------+-------+-------+-----+
                        |---( # of entries)---|
```

- EntryCount(VInt): number of entries
- StartPos(VInt): the start pos of the first (data) block referenced by this (index) block
- Entry (IndexEntry)

### Entry
```
+----------+--------------+
| BlockLen | FirstOrdinal |
+----------+--------------+
```
- BlockLen(VInt): length of the block
- FirstOrdinal(VInt): ordinal of the first element in the given block
