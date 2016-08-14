% Tutorial: Indexing Wikipedia with Tantivy CLI

# Introduction

In this tutorial, we will create a brand new index
with the articles of English wikipedia in it.


# Install

There are two ways to get `tantivy`.
If you are a rust programmer, you can run `cargo install tantivy`.
Alternatively, if you are on `Linux 64bits`, you can download a
static binary:  [binaries/linux_x86_64/](http://fulmicoton.com/tantivy/binaries/linux_x86_64/tantivy) 

# Creating the index

Create a directory in which your index will be stored.

```bash
    # create the directory
    mkdir wikipedia-index
```


We will now initialize the index and create it's schema.

Our documents will contain
* a title
* a body 
* a url

Running `tantivy new` will start a wizard that will help you go through
the definition of the schema of our new index.

```bash
    tantivy new -i wikipedia-index
```

When asked answer to the question as follows:

```none

    Creating new index 
    Let's define it's schema! 



    New field name  ? title
    Text or unsigned 32-bit Integer (T/I) ? T
    Should the field be stored (Y/N) ? Y
    Should the field be indexed (Y/N) ? Y
    Should the field be tokenized (Y/N) ? Y
    Should the term frequencies (per doc) be in the index (Y/N) ? Y
    Should the term positions (per doc) be in the index (Y/N) ? Y
    Add another field (Y/N) ? Y



    New field name  ? body
    Text or unsigned 32-bit Integer (T/I) ? T
    Should the field be stored (Y/N) ? Y
    Should the field be indexed (Y/N) ? Y
    Should the field be tokenized (Y/N) ? Y
    Should the term frequencies (per doc) be in the index (Y/N) ? Y
    Should the term positions (per doc) be in the index (Y/N) ? Y
    Add another field (Y/N) ? Y



    New field name  ? url
    Text or unsigned 32-bit Integer (T/I) ? T
    Should the field be stored (Y/N) ? Y
    Should the field be indexed (Y/N) ? N
    Add another field (Y/N) ? N

    [
    {
        "name": "title",
        "type": "text",
        "options": {
        "indexing": "position",
        "stored": true
        }
    },
    {
        "name": "body",
        "type": "text",
        "options": {
        "indexing": "position",
        "stored": true
        }
    },
    {
        "name": "url",
        "type": "text",
        "options": {
        "indexing": "unindexed",
        "stored": true
        }
    }
    ]


```

If you want to know more about the meaning of these options, you can check out the [schema doc page](http://fulmicoton.com/tantivy/tantivy/schema/index.html).  

The json displayed at the end has been written in `wikipedia-index/meta.json`.


# Get the documents to index

Tantivy's index command offers a way to index a json file.
More accurately, the file must contain one document per line, in a json format.
The structure of this JSON object must match that of our schema definition.

```json
    {"body": "some text", "title": "some title", "url": "http://somedomain.com"}
```

You can download a corpus of more than 5 millions articles from wikipedia 
formatted in the right format here : [wiki-articles.json (2.34 GB)](https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=0).
If you are in a rush you can [download 100 articles in the right format here](http://fulmicoton.com/tantivy/tutorial/wiki-articles-first100.json).

Make sure to uncompress the file

```bash
    bunzip2 wiki-articles.json.bz2
``` 

# Index the documents.

The `index` command will index your document.
By default it will use as many threads as there are core on your machine.

On my computer (8 core Xeon(R) CPU X3450  @ 2.67GHz), it only takes 7 minutes.

```
    cat /data/wiki-articles | tantivy index -i wikipedia-index
```

While it is indexing, you can peek at the index directory
to check what is happening.

```bash
    ls wikipedia-index
```

If you indexed the 5 millions articles, you should see a lot of files, all with the following format
The main file is `meta.json`.

Our index is in fact divided in segments. Each segment acts as an individual smaller index.
It is named by a uuid. 
Each different files is storing a different datastructure for the index.


# Serve the search index

```
    tantivy serve -i wikipedia-index
```

You can start a small server with a JSON API to search into wikipedia.
By default, the server is serving on the port `3000`.


