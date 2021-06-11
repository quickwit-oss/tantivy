

# Fast Field Codecs

This crate contains various fast field codecs, used to compress/decompress fast field data in tantivy.

## Contributing

Contributing is pretty straightforward. Since the bitpacking is the simplest compressor, you can check it for reference.

A codec needs to implement 3 parts:

A reader implementing `CodecReader` to read the codec.
A serializer implementing `FastFieldSerializerEstimate` for compression estimation.
`CodecId`, to identify the codec.


