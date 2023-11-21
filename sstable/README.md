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
+-----+----------------+-------------+-------------+---------+---------+
| Fst | BlockAddrStore | StoreOffset | IndexOffset | NumTerm | Version |
+-----+----------------+-------------+-------------+---------+---------+
```
- Fst(Fst): finit state transducer mapping keys to a block number
- BlockAddrStore(BlockAddrStore): store mapping a block number to its BlockAddr
- FstLen(u64): Lenght of the Fst
- IndexOffset(u64): Offset to the start of the SSTFooter
- NumTerm(u64): number of terms in the sstable
- Version(u32): Currently equal to 3

### Fst

Fst is in the format of tantivy\_fst

### BlockAddrStore

+---------+-----------+-----------+-----+-----------+-----------+-----+
| MetaLen | BlockMeta | BlockMeta | ... | BlockData | BlockData | ... |
+---------+-----------+-----------+-----+-----------+-----------+-----+
          |---------(N blocks)----------|---------(N blocks)----------|

- MetaLen(u64): lenght of the BlockMeta section
- BlockMeta(BlockAddrBlockMetadata): metadata to seek through BlockData
- BlockData(CompactedBlockAddr): bitpacked per block metadata

### BlockAddrBlockMetadata

+--------+------------+---------------+-----------+-------------+-------------------+-----------------+----------+
| Offset | RangeStart | FirstOrdianal | RangeSlop | OrdinalSlop | FirstOrdinalNBits | RangeStartNBits | BlockLen |
+--------+------------+---------------+-----------+-------------+-------------------+-----------------+----------+

- Offset(u64): offset of the corresponding BlockData in the datastream
- RangeStart(u64): the start position of the first block
- FirstOrdinal(u64): the first ordinal of the first block
- RangeSlop(u32): slop predicted for start range evolution (see computation in BlockData)
- OrdinalSlop(u64): slop predicted for first ordinal evolution (see computation in BlockData)
- FirstOrdinalNBits(u8): number of bits per ordinal in datastream (see computation in BlockData)
- RangeStartNBits(u8): number of bits per range start in datastream (see computation in BlockData)

### BlockData

+-----------------+-------------------+---------------+
| RangeStartDelta | FirstOrdinalDelta | FinalRangeEnd |
+-----------------+-------------------+---------------+
|------(BlockLen repetitions)---------|

- RangeStartDelta(var): RangeStartNBits *bits* of little endian number. See below for decoding
- FirstOrdinalDelta(var): FirstOrdinalNBits *bits* of little endian number. See below for decoding
- FinalRangeEnd(var): RangeStartNBits *bits* of integer. See below for decoding

converting a BlockData of index Index and a BlockAddrBlockMetadata to an actual block address is done as follow:
range\_prediction := RangeStart + Index * RangeSlop;
range\_derivation := RangeStartDelta - (1 << (RangeStartNBits-1));
range\_start := range\_prediction + range\_derivation

the same computation can be done for ordinal
