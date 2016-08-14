position not stored
lenient mode for query parser
phrase queries
documentation
query explain with proper term names
better schema JSON
better error for opening index
error handling in the writer

 

Arc for the schema
error management
add merge policy
find solution to "I have a docaddress but the segment does not exist anymore problem"

pass over offset from previous block

test untokenized

but empty field value for last segment
test field with more than one value
doc values for other types
use skip list for each blocks
find a clear way to put the tokenized/untokenized thing upstream
index frequent bigrams
reconsider the first byte == field in the [u8] repr of a term.
good cli
good cli based demo
intersection
WAND
rethink query iteration mechanics / API (should we setScorer, should
collector take different objects?)
Dig issue monoids idea for collectors
sort by fast field
date
geo search
deletes
last fieldnorm
avoid copy from hashmap
settable number of docs before committing
cascade committing segments

parametrable number of docs before committing