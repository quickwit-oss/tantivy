![Tantivy](http://fulmicoton.com/tantivy_500.png#h)

[![Build Status](https://travis-ci.org/fulmicoton/tantivy.svg?branch=master)](https://travis-ci.org/fulmicoton/tantivy)
[![Coverage Status](https://coveralls.io/repos/github/fulmicoton/tantivy/badge.svg?branch=master)](https://coveralls.io/github/fulmicoton/tantivy?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)



**Tantivy** is a **full text search engine library** written in rust.

It is strongly inspired by Lucene's design.

# Features

- configurable indexing (optional term frequency and position indexing)
- Tf-Idf scoring
- Basic query language
- Incremental indexing
- Multithreaded indexing (indexing en wikipedia takes 4mn on my desktop)
- Mmap based
- SIMD integer compression
- u32 fast fields (equivalent of doc values in Lucene)
- LZ4 compressed document store
- Cheesy logo with a horse

# Getting started

- [tantivy's usage example](http://fulmicoton.com/tantivy-examples/simple_search.html)
- [tantivy-cli and its tutorial](https://github.com/fulmicoton/tantivy-cli).
It will walk you through getting a wikipedia search engine up and running in a few minutes.
- [reference doc](http://fulmicoton.com/tantivy/tantivy/index.html).


# Compiling 

Tantivy has a git submodule called `simdcomp`.
After cloning the repository, you will need to initialize and update
the submodules. The project can then be built using `cargo`.

    git clone git@github.com:fulmicoton/tantivy.git
    git submodule init
    git submodule update
    cargo build



# Contribute

Send me an email (paul.masurel at gmail.com) if you want to contribute to tantivy. 
