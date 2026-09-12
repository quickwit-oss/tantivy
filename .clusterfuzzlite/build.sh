#!/bin/bash -eu
#
# ClusterFuzzLite build script: compiles the cargo-fuzz targets and installs the
# resulting binaries into $OUT, where ClusterFuzzLite expects to find them.

cd "$SRC/tantivy"

# -O builds with optimisations; --debug-assertions keeps overflow checks and
# debug_assert! live, which is where a lot of the interesting bugs surface.
cargo fuzz build -O --debug-assertions

# cargo-fuzz has moved its output directory between releases, so accept either
# layout rather than pinning one.
candidate_dirs=(
    "fuzz/target/x86_64-unknown-linux-gnu/release"
    "target/x86_64-unknown-linux-gnu/release"
)

# Derive the target list from the sources, so a newly added fuzz target is
# picked up here automatically.
for source_file in fuzz/fuzz_targets/*.rs; do
    target_name="$(basename "${source_file%.*}")"

    binary=""
    for dir in "${candidate_dirs[@]}"; do
        if [ -f "$dir/$target_name" ]; then
            binary="$dir/$target_name"
            break
        fi
    done

    if [ -z "$binary" ]; then
        echo "ERROR: no built binary found for fuzz target '$target_name'." >&2
        echo "       Looked in: ${candidate_dirs[*]}" >&2
        exit 1
    fi

    cp "$binary" "$OUT/$target_name"
    chmod +x "$OUT/$target_name"
    echo "Installed $target_name from $binary"
done
