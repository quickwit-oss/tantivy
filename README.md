[![Build Status](https://travis-ci.org/fulmicoton/tantivy.svg?branch=master)](https://travis-ci.org/fulmicoton/tantivy)
[![Coverage Status](https://coveralls.io/repos/github/fulmicoton/tantivy/badge.svg?branch=master)](https://coveralls.io/github/fulmicoton/tantivy?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# Tantivy

Check out the [doc](http://fulmicoton.com/tantivy/tantivy/index.html)


**Tantivy** is a **text search engine library** written in rust. It also comes with a command-line CLI that can help you get an up and running search engine
in minutes.


# Getting started with the command-line util



## Creating a new index 


```bash
    # my-index directory must be created beforehands
    mkdir my-index
     
    # Tantivy new will trigger a wizard, to define the schema
    # of your index. 
    tantivy new -i ./my-index
```

## Index your documents

