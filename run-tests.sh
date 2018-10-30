#!/bin/bash
cargo test --no-default-features --features mmap -- --test-threads 1
