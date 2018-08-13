#!/usr/bin/env bash

# This script takes care of testing your crate

set -ex

main() {
    if [ ! -z $CODECOV ]; then
        echo "Codecov"
        cargo build --verbose && cargo coverage --verbose && bash <(curl -s https://codecov.io/bash) -s target/kcov
    else
        echo "Build"
        cross build --target $TARGET
        cross build --target $TARGET --release
        if [ ! -z $DISABLE_TESTS ]; then
            return
        fi
        echo "Test"
        cross test --target $TARGET
    fi
}

# we don't run the "test phase" when doing deploys
if [ -z $TRAVIS_TAG ]; then
    main
fi
