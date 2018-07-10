# This script takes care of testing your crate

set -ex

main() {
    cross build --target $TARGET
    cross build --target $TARGET --release

    if [ ! -z $DISABLE_TESTS ]; then
        return
    fi

    if [ -z $CODECOV ]; then
        cargo build --verbose && cargo coverage --verbose && bash <(curl -s https://codecov.io/bash) -s target/kcov
    fi

    cross test --target $TARGET
    # cross test --target $TARGET --release

    # cross run --target $TARGET
    # cross run --target $TARGET --release
}

# we don't run the "test phase" when doing deploys
if [ -z $TRAVIS_TAG ]; then
    main
fi
