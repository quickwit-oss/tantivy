# Indexing Wikipedia with Tantivy CLI interface

## Introduction

In this tutorial, we will create a brand new index
with the articles of English wikipedia in it.

 
 
## Step 1 - Get tantivy CLI interface

There are two ways to get `tantivy`.
If you are a rust programmer, you can run `cargo install tantivy`.
Alternatively, if you are on `Linux 64bits`, you can download a
static binary here []() 

## Step 2 - creating the index

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

Running

```bash
    # create the directory
    tantivy 
```
   
   

https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=0