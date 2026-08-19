- bugfix: (EQ my_col 2i64) type inference should return that my_col is expected to be a number.
- add support for null in return value (valid for regex, not matching and matching empty string are two different things)
- add support for null in args
- add support for building string (LOWER etc.) -> will require some arena owned by the function.
- identify all required functions (actually in use)
- identify the spec of the different functions
- implement the different functions.
- optimize regex: we receive REGEXP_EXTRACT expressions. Nowadays this REGEXP_EXTRACT are likely generated via our AI-generated
grok parsing rules. That means they typically have more groups than actually necessary (at least in aggregation and search predicate).
For that iteration at least, we embrace the "event query" way and deal with all REGEXP_EXTRACT calls as if they were independent and do not compute
them jointly for optimization. Still, we should make sure we remove the needless groups before compiling the regex.
- cache compilation (column types should be part of the cache key)
- calculated predicate scorer intiialization.


==========================
* protobuf conversion to untypedexpr 

* building query object -(pomsky)-> query ast (quickwit) -(quickwit)-> tantivy query 

* populate warmup info

* integration of expr into aggregation
  * rewrite query over calculated field as predicate

* implement fetch one
  * fetch one UI subtlety risk:!? there might be some subtlety we are missing here. calculated fields seem to be loaded in a second time. I expect
this is just an extra projection parameter we can just ignore.

* expression computation: done
  * handle null *done*
  * add functions
  * handle string arena  *done*

* implement the plain string column format
