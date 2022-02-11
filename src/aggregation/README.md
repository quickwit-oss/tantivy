# Code Organization

The code is organized in submodules:

##bucket
Contains all bucket aggregations, like range aggregation. These bucket aggregations group documents into buckets and can contain sub-aggegations.

##metric
Contains all metric aggregations, like average aggregation. Metric aggregations do not have sub aggregations.

#### agg_req
agg_req contains the users aggregation request.

#### agg_req_with_accessor
agg_req_with_accessor contains the users aggregation request eriched with fast field accessors etc, which are
used during collection.

#### segment_agg_result
segment_agg_result is the aggregation result tree during collection in a segment.
It will get the tree from agg_req_with_accessor passed during collection in a segment.

#### intermediate_agg_result
intermediate_agg_result is the aggregation tree for merging with other trees.

#### agg_result
agg_result is the final aggregation tree.

