# Code Organization

The code is organized in submodules:

##bucket
Contains all bucket aggregations, like range aggregation. These bucket aggregations group documents into buckets and can contain sub-aggegations.

##metric
Contains all metric aggregations, like average aggregation. Metric aggregations do not have sub aggregations.

#### agg_req
agg_req contains the users aggregation request.

#### agg_req_with_accessor
agg_req_with_accessor contains the users aggregation request enriched with fast field accessors etc, which are
used during collection.

#### segment_agg_result
segment_agg_result contains the aggregation result tree, which is used for collection of a segment.
The tree from agg_req_with_accessor is passed during collection.

#### intermediate_agg_result
intermediate_agg_result contains the aggregation tree for merging with other trees.

#### agg_result
agg_result contains the final aggregation tree.

