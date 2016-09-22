![Tantivy](http://fulmicoton.com/tantivy.png#hash =500)

[![Build Status](https://travis-ci.org/fulmicoton/tantivy.svg?branch=master)](https://travis-ci.org/fulmicoton/tantivy)
[![Coverage Status](https://coveralls.io/repos/github/fulmicoton/tantivy/badge.svg?branch=master)](https://coveralls.io/github/fulmicoton/tantivy?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


**Tantivy** is a **text search engine library** written in rust.
Without being exactly a port of Lucene in Rust, it is strongly inspired by Lucene's design.


To get started with tantivy, you should check out [tantivy's usage example](http://fulmicoton.com/tantivy-examples/simple_search.html)

You might also be interested in [tantivy-cli](https://github.com/fulmicoton/tantivy-cli) and its [tutorial](https://github.com/fulmicoton/tantivy-cli).
It will walk you through getting a wikipedia search engine up and running in a few minutes.


# Compiling 

`simdcomp` has a submodule.
After cloning the repository, you will need to initialize and update
the submodules. The project can then be build using `cargo`.

    git clone git@github.com:fulmicoton/tantivy.git
    git submodule init
    git submodule update
    cargo build


Check out the [reference doc](http://fulmicoton.com/tantivy/tantivy/index.html).
You may also want to have look at [tantivy-cli and its tutorial](https://github.com/fulmicoton/tantivy-cli) , a command-line util for tantivy.


# Contribute

Send me an email (paul.masurel at gmail.com) if you want to contribute to tantivy. 
