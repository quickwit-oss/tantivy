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

The groups below are mutually exclusive and cover all 55 functions in the inventory.

## Status summary

| Status | Count |
|---|---:|
| Supported | 38 |
| Not supported yet but apparently not used in SaaS | 0 |
| Not supported yet because targeting arrays | 6 |
| Not supported yet because of specification ambiguity | 3 |
| Not supported yet because of a current framework limitation | 3 |
| Not supported yet because of implementation complexity | 5 |
| **Total** | **55** |

## Supported (38)

These functions are implemented and covered by unit tests.

| Tag | Function |
|---:|---|
| 1 | `AND` |
| 2 | `OR` |
| 3 | `NOT` |
| 4 | `ADD` |
| 5 | `SUBTRACT` |
| 6 | `MULTIPLY` |
| 7 | `DIVIDE` |
| 8 | `EQ` |
| 9 | `GT` |
| 10 | `LT` |
| 11 | `GT_EQ` |
| 12 | `LT_EQ` |
| 13 | `IS_NULL` |
| 14 | `IS_NOT_NULL` |
| 16 | `UPPER` |
| 17 | `LOWER` |
| 19 | `CONCAT` |
| 20 | `TEXT_JOIN` |
| 22 | `NEQ` |
| 24 | `INT_MOD` |
| 25 | `ABS` |
| 27 | `ROUND` |
| 28 | `FLOOR` |
| 29 | `CEIL` |
| 34 | `POW` |
| 35 | `SQRT` |
| 38 | `MIN` |
| 39 | `MAX` |
| 40 | `LEFT` |
| 41 | `RIGHT` |
| 42 | `SUBSTRING` |
| 43 | `SPLIT_BEFORE` |
| 44 | `SPLIT_AFTER` |
| 50 | `REGEXP_EXTRACT` |
| 54 | `TRIM` |
| 55 | `IF` |
| 79 | `SUBSTRING_COUNT` |
| 80 | `REGEXP_LIKE` |

## Not supported yet but apparently not used in SaaS (0)

None. The source inventory proves that each listed function is reachable through the SaaS reader
path, but it does not contain runtime usage telemetry. No function can therefore be placed in this
group with defensible evidence.

## Not supported yet because targeting arrays (6)

| Tag | Function | Reason |
|---:|---|---|
| 21 | `IN` | This calculated-field operation is lowered to array containment. |
| 70 | `ARRAY_CONTAINS` | Requires array values and array-aware typing. |
| 71 | `ARRAY_SUM` | Requires numeric array values and array-aware typing. |
| 72 | `ARRAY_AVG` | Requires numeric array values and array-aware typing. |
| 73 | `ARRAY_OF` | Produces an array value, which `VarType` cannot currently represent. |
| 76 | `ARRAY_CONTAINS_NULLABLE` | Requires nullable array-element semantics. |

## Not supported yet because of specification ambiguity (3)

| Tag | Function | Reason |
|---:|---|---|
| 66 | `EXTRACT` | The type checker says string while the function registry says `int64`; failure behavior is also unresolved. |
| 67 | `SEMVER` | Malformed-version behavior is uncharacterized, and its tag collides with `TIMESTAMP_DIFF` in another schema revision. |
| 74 | `TIMESTAMP_DIFF` | Schema revisions disagree on its tag, and the exact temporal parsing/rendering contract remains unresolved. |

## Not supported yet because of a current framework limitation (3)

| Tag | Function | Reason |
|---:|---|---|
| 56 | `COALESCE` | Its n-ary common-type selection differs from the current binary unification rules and needs a broader coercion refactor. |
| 61 | `TRY_CAST_INT` | Correct behavior needs cast-elision/tree rewriting plus a dedicated nullable parsing/coercion path. |
| 63 | `TRY_CAST_FLOAT` | Shares the cast-elision/tree-rewrite requirement and needs its own parsing heuristic. |

## Not supported yet because of implementation complexity (5)

| Tag | Function | Reason |
|---:|---|---|
| 15 | `CIDR` | Requires variadic compile-time IPv4/IPv6 mask parsing, zone handling, and structural argument restrictions. |
| 18 | `PROPER` | Requires locale-aware title casing with compatible Unicode segmentation and casing data. |
| 65 | `TO_TIMESTAMP` | Requires a custom format-language translator plus compatible parsing, failure, and rendering behavior. |
| 77 | `LEVENSHTEIN_DISTANCE` | Its kernel and Unicode/error edge cases still need characterization before implementing the distance algorithm. |
| 78 | `ENTROPY` | Its metric kernel and edge cases are still uncharacterized. |
