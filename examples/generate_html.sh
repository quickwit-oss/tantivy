#!/bin/bash

for example in $(ls *.rs)
do
    docco $example -o html
done
