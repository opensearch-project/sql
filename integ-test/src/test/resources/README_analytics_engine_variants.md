# Analytics-engine (`_ae`) dataset variants

The analytics-engine backend (DataFusion, reached when an index is parquet-backed —
see `RestUnifiedQueryAction.isAnalyticsIndex`) cannot read `nested`, `geo_point`, or
`geo_shape` fields. ITs run against the analytics engine by setting
`-Dtests.analytics.parquet_indices=true`, which makes every test-created index
parquet-backed.

When that flag is on, `TestUtils.AnalyticsIndexConfig.resolveDatasetPath(...)`
transparently swaps a resource path `foo.json` → `foo_ae.json` **iff** the `_ae`
sibling exists on disk. With the flag off (normal CI), the swap never happens and the
original files are used byte-for-byte — so a dataset only needs an `_ae` variant if
its fields are actually unsupported, and adding one is a pure opt-in.

Each `_ae` variant **drops** every `nested`/`geo_point`/`geo_shape` field from the
mapping and the matching keys from each bulk doc. (Policy: drop the field entirely —
no `nested → object` demotion.)

## Variants present (a useful, non-empty subset survives the drop)

| Source dataset | Dropped field(s) | Surviving columns |
| --- | --- | --- |
| `datatypes` (DATA_TYPE_NONNUMERIC) | `nested_value` (nested), `geo_point_value` (geo_point) | boolean/keyword/text/binary/date/date_nanos/ip/object |
| `nested_simple` (NESTED_SIMPLE) | `address` (nested) | `name`, `age`, `id` |
| `deep_nested` (DEEP_NESTED) | `projects` (nested); `accounts` data left out (not in explicit mapping) | `city`, `account` objects |
| `merge_test_1` (MERGE_TEST_1) | `machine_array` (nested) | `machine`, `machine_deep` objects |
| `merge_test_2` (MERGE_TEST_2) | `machine_array` (nested) | `machine`, `machine_deep` objects |

## Deliberately NOT given a variant (assume-skip under AE instead)

These datasets are *entirely* nested-centric — dropping the unsupported field leaves an
empty mapping/data, so a variant would restore no coverage. The tests that load them are
nested/mvexpand tests that cannot pass under AE regardless; they should assume-skip when
`isAnalyticsParquetIndicesEnabled()` is true rather than load a hollow index.

- `cascaded_nested` (CASCADED_NESTED) — the whole mapping is one `author` nested tree.
- `mvexpand_edge_cases` (MVEXPAND_EDGE_CASES) — `skills` nested is the mvexpand target.

## Adding a new variant

1. Copy the mapping to `indexDefinitions/<name>_ae.json`, remove unsupported fields.
2. Copy the bulk data to `<name>_ae.json`, remove the matching keys from each doc.
3. Nothing else — `resolveDatasetPath` finds it automatically when AE is enabled.
