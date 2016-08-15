[![Build Status](https://travis-ci.org/fulmicoton/tantivy.svg?branch=master)](https://travis-ci.org/fulmicoton/tantivy)
[![Coverage Status](https://coveralls.io/repos/github/fulmicoton/tantivy/badge.svg?branch=master)](https://coveralls.io/github/fulmicoton/tantivy?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# Tantivy

Check out the [doc](http://fulmicoton.com/tantivy/tantivy/index.html)


**Tantivy** is a **text search engine library** written in rust. It also comes with a command-line CLI that can help you get an up and running search engine
in minutes.


# How it works

This document explains how tantivy works, and specifically 
what kind of datastructures are used to index and store the data.

# An inverted index

As you may know, an idea central to search engines is to assign a document id 
to each document, and build an inverted index, which is simply
a datastructure associating each term (word) to a sorted list of doc ids.   

Such an index then makes it possible to compute the union or
the intersection of the documents containing two terms
in `O(1)` memory and `O(n)` time.

## Term dictionary

Tantivy term dictionary (`.term` files) are stored in
a finite state transducer (courtesy of the excellent
[`fst`](https://github.com/BurntSushi/fst) crate).

For each term, the dictionary associates
a [TermInfo](http://fulmicoton.com/tantivy/tantivy/postings/struct.TermInfo.html). 
which contains all of the information required to access the list of doc ids of the doc containing
the term.

In fact `fst` can only associated terms to a long. [`FstMap`](https://github.com/fulmicoton/tantivy/blob/master/src/datastruct/fstmap.rs) are
in charge to build a KV map on top of it.  


## Postings

The posting lists (sorted list of doc ids) are encoded in the `.idx` file.
Optionally, you specify in your schema that you want tf-idf to be encoded
in the index file (if you do not, the index will behave as if all documents
have a term frequency of 1).
Tf-idf scoring requires the term frequency (number of time the term appeared in the field of the document)
for each document.


# Segments

Tantivy's index are divided into segments.
All segments are as many independent structure.

This has many benefits. For instance, assuming you are
trying to one billion documents, you could split
your corpus into N pieces, index them on Hadoop, copy all
of the resulting segments in the same directory 
and edit the index meta.json file to list all of the segments.

This strong division also simplify a lot multithreaded indexing.
Each thread is actually build its own segment.


## 

# Store

The store 
When a document  
