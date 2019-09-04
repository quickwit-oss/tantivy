#!/usr/bin/env bash

# This script takes care of testing your crate

set -ex

main() {
    if [ ! -z $CODECOV ]; then
        echo "Codecov"
        cargo build --verbose && cargo coverage --verbose --all && bash <(curl -s https://codecov.io/bash) -s target/kcov
    else
        echo "Build"
        cross build --target $TARGET
        if [ ! -z $DISABLE_TESTS ]; then
            return
        fi
        echo "Test"
        cross test --target $TARGET --no-default-features --features mmap
        cross test --target $TARGET --no-default-features --features mmap query-grammar
    fi
    for example in $(ls examples/*.rs)
    do
        cargo run --example  $(basename $example .rs)
    done
}

# we don't run the "test phase" when doing deploys
if [ -z $TRAVIS_TAG ]; then
    main
fi
