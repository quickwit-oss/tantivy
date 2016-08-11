#!/bin/bash
DEST=target/doc/tantivy/docs/
mkdir -p $DEST

for f in $(ls docs/*.md)
do
    rustdoc $f -o $DEST --markdown-css ../../rustdoc.css --markdown-css style.css
done

cp docs/*.css $DEST