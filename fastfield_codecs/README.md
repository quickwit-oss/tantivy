

# Fast Field Codecs

This crate contains various fast field codecs, used to compress/decompress fast field data in tantivy.

## Contributing

Contributing is pretty straightforward. Since the bitpacking is the simplest compressor, you can check it for reference.

A codec needs to implement 2 traits:

- A reader implementing `FastFieldCodecReader` to read the codec.
- A serializer implementing `FastFieldCodecSerializer` for compression estimation and codec name + id.

### Tests

Once the traits are implemented test and benchmark integration is pretty easy (see `test_with_codec_data_sets` and `bench.rs`).

Make sure to add the codec to the main.rs, which tests the compression ratio and estimation against different data sets. You can run it with:
```
cargo run --features bin
```

### TODO
- Add real world data sets in comparison
- Add codec to cover sparse data sets


### Codec Comparison
```
+----------------------------------+-------------------+------------------------+
|                                  | Compression Ratio | Compression Estimation |
+----------------------------------+-------------------+------------------------+
| Autoincrement                    |                   |                        |
+----------------------------------+-------------------+------------------------+
| LinearInterpol                   | 0.000039572664    | 0.000004396963         |
+----------------------------------+-------------------+------------------------+
| MultiLinearInterpol              | 0.1477348         | 0.17275847             |
+----------------------------------+-------------------+------------------------+
| Bitpacked                        | 0.28126493        | 0.28125                |
+----------------------------------+-------------------+------------------------+
| Monotonically increasing concave |                   |                        |
+----------------------------------+-------------------+------------------------+
| LinearInterpol                   | 0.25003937        | 0.26562938             |
+----------------------------------+-------------------+------------------------+
| MultiLinearInterpol              | 0.190665          | 0.1883836              |
+----------------------------------+-------------------+------------------------+
| Bitpacked                        | 0.31251436        | 0.3125                 |
+----------------------------------+-------------------+------------------------+
| Monotonically increasing convex  |                   |                        |
+----------------------------------+-------------------+------------------------+
| LinearInterpol                   | 0.25003937        | 0.28125438             |
+----------------------------------+-------------------+------------------------+
| MultiLinearInterpol              | 0.18676           | 0.2040086              |
+----------------------------------+-------------------+------------------------+
| Bitpacked                        | 0.31251436        | 0.3125                 |
+----------------------------------+-------------------+------------------------+
| Almost monotonically increasing  |                   |                        |
+----------------------------------+-------------------+------------------------+
| LinearInterpol                   | 0.14066513        | 0.1562544              |
+----------------------------------+-------------------+------------------------+
| MultiLinearInterpol              | 0.16335973        | 0.17275847             |
+----------------------------------+-------------------+------------------------+
| Bitpacked                        | 0.28126493        | 0.28125                |
+----------------------------------+-------------------+------------------------+

```
