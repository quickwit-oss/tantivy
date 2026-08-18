# Calculated-field function implementation status

This is the implementation queue for the calculated-field functions that are usable on the
current production expression path. It was derived from these `prod` checkouts on 2026-08-18:

- `dd-go` at `711bff42e80321b78d50fc35f7b89a3af44b47fd`
- `logs-backend` at `a817d32653e59ce4c50c96f3d28e49546a5c16a8`

The relevant source paths and their roles are documented in
[`calc_fields/phase0+1.md`](calc_fields/phase0+1.md#1-source-files-path-aliases-used-throughout).
The inventory below is the intersection classified as `LIVE`: accepted by the logs-backend Java
surface, emitted by `ExprNodeToProto`, and backed by the dd-go reader implementation. This yields
55 functions. `REGEXP_REPLACE` is intentionally excluded because the producer emits it but dd-go
does not register or execute it. Reader-only functions rejected by the CalcNode producer are also
excluded.

Status legend: **done** is implemented and covered by unit tests; **pending** is in the queue;
**out-of-scope** is deliberately deferred because it requires array support or was explicitly
excluded from this pass.

| Tag | Function | Status |
|---:|---|---|
| 1 | `AND` | done |
| 2 | `OR` | done |
| 3 | `NOT` | done |
| 4 | `ADD` | done |
| 5 | `SUBTRACT` | pending |
| 6 | `MULTIPLY` | pending |
| 7 | `DIVIDE` | pending |
| 8 | `EQ` | done |
| 9 | `GT` | pending |
| 10 | `LT` | pending |
| 11 | `GT_EQ` | pending |
| 12 | `LT_EQ` | pending |
| 13 | `IS_NULL` | done |
| 14 | `IS_NOT_NULL` | done |
| 15 | `CIDR` | pending |
| 16 | `UPPER` | pending |
| 17 | `LOWER` | done |
| 18 | `PROPER` | pending |
| 19 | `CONCAT` | pending |
| 20 | `TEXT_JOIN` | pending |
| 21 | `IN` | pending |
| 22 | `NEQ` | pending |
| 24 | `INT_MOD` | pending |
| 25 | `ABS` | pending |
| 27 | `ROUND` | pending |
| 28 | `FLOOR` | pending |
| 29 | `CEIL` | pending |
| 34 | `POW` | pending |
| 35 | `SQRT` | pending |
| 38 | `MIN` | pending |
| 39 | `MAX` | pending |
| 40 | `LEFT` | pending |
| 41 | `RIGHT` | pending |
| 42 | `SUBSTRING` | pending |
| 43 | `SPLIT_BEFORE` | pending |
| 44 | `SPLIT_AFTER` | pending |
| 50 | `REGEXP_EXTRACT` | done |
| 54 | `TRIM` | pending |
| 55 | `IF` | pending |
| 56 | `COALESCE` | pending |
| 61 | `TRY_CAST_INT` | pending |
| 63 | `TRY_CAST_FLOAT` | pending |
| 65 | `TO_TIMESTAMP` | pending |
| 66 | `EXTRACT` | pending |
| 67 | `SEMVER` | pending |
| 70 | `ARRAY_CONTAINS` | out-of-scope |
| 71 | `ARRAY_SUM` | out-of-scope |
| 72 | `ARRAY_AVG` | out-of-scope |
| 73 | `ARRAY_OF` | out-of-scope |
| 74 | `TIMESTAMP_DIFF` | pending |
| 76 | `ARRAY_CONTAINS_NULLABLE` | out-of-scope |
| 77 | `LEVENSHTEIN_DISTANCE` | out-of-scope |
| 78 | `ENTROPY` | out-of-scope |
| 79 | `SUBSTRING_COUNT` | pending |
| 80 | `REGEXP_LIKE` | pending |

Progress: **9 / 48 in-scope** functions implemented; **7** functions are out-of-scope.
