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
**out-of-scope** is deliberately deferred because it requires array support, was explicitly
excluded from this pass, or is complex enough to warrant a separate implementation pass.

| Tag | Function | Status |
|---:|---|---|
| 1 | `AND` | done |
| 2 | `OR` | done |
| 3 | `NOT` | done |
| 4 | `ADD` | done |
| 5 | `SUBTRACT` | done |
| 6 | `MULTIPLY` | done |
| 7 | `DIVIDE` | done |
| 8 | `EQ` | done |
| 9 | `GT` | done |
| 10 | `LT` | done |
| 11 | `GT_EQ` | done |
| 12 | `LT_EQ` | done |
| 13 | `IS_NULL` | done |
| 14 | `IS_NOT_NULL` | done |
| 15 | `CIDR` | out-of-scope |
| 16 | `UPPER` | done |
| 17 | `LOWER` | done |
| 18 | `PROPER` | out-of-scope |
| 19 | `CONCAT` | done |
| 20 | `TEXT_JOIN` | done |
| 21 | `IN` | out-of-scope |
| 22 | `NEQ` | done |
| 24 | `INT_MOD` | done |
| 25 | `ABS` | done |
| 27 | `ROUND` | out-of-scope |
| 28 | `FLOOR` | out-of-scope |
| 29 | `CEIL` | out-of-scope |
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

Progress: **22 / 42 in-scope** functions implemented; **13** functions are out-of-scope.

## Deferred implementation notes

- `CIDR` requires compile-time parsing of variadic IPv4/IPv6 network masks, exact IP parsing
  compatibility (including IPv6 zone-index stripping), and structural enforcement that argument 0
  is a bare column while every remaining argument is a string constant. It is deferred as a
  complex function rather than expanding this scalar-function pass.
- `PROPER` is locale-aware title casing using Go's `cases.Title(language.AmericanEnglish)`. Exact
  compatibility requires Unicode word segmentation, American-English title-case rules, and the
  matching Unicode data version; Rust's standard library does not provide this operation.
- Calculated-field `IN` is lowered by logs-backend to dd-go's `ARRAY_CONTAINS`; it is distinct from
  the query-language `InExprNode` set-membership path and is deferred with the other array
  functions.
- `ROUND` has a literal-value-dependent return type, four integer/float input-output paths, an
  aborting float-to-int overflow case, and an observable production defect where integer input at
  precision zero leaves the lazily allocated output unwritten. It needs a separate parity decision
  and implementation pass.
- `FLOOR` has the same unwritten lazy-output defect for integer inputs. Its float path converts
  NaN, infinities, and out-of-range results directly to `int64`, whose exact Go result is
  architecture-dependent. It is deferred pending an explicit production-parity policy.
- `CEIL` shares `FLOOR`'s unwritten integer-output defect and architecture-dependent exceptional
  float-to-`int64` conversions, so it is deferred under the same parity policy.
