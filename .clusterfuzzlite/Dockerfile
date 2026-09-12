# ClusterFuzzLite build image for Tantivy fuzzing
# Builds the Rust fuzz targets with cargo-fuzz and sanitizer instrumentation

FROM gcr.io/oss-fuzz-base/base-builder-rust

COPY . $SRC/tantivy
WORKDIR $SRC/tantivy
COPY .clusterfuzzlite/build.sh $SRC/
