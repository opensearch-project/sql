# union recursive

## Description

The `union recursive` command evaluates a recursive subsearch and unions its output with the
anchor pipeline until a fixpoint or configured limits are reached. This is the PPL equivalent of
SQL `WITH RECURSIVE`.

## Syntax

<anchor_pipeline>
| union recursive name=<relation_name> [max_depth=<N>] [max_rows=<M>]
  [ <recursive_subsearch> ]
| <rest_of_pipeline>

- name: required. Logical name of the recursive relation.
- max_depth: optional. Maximum number of recursive iterations.
- max_rows: optional. Maximum total rows produced by the recursive relation.
- recursive_subsearch: required. Bracketed subsearch that must reference the recursive relation and
  return the same schema as the anchor.

## Usage notes

- The anchor pipeline defines the base rows (iteration 0).
- The recursive subsearch is evaluated repeatedly and unioned with the anchor output.
- `union recursive` uses UNION ALL semantics; duplicates are not removed.
- The recursive relation name is only visible inside the bracketed recursive block.

## Example: Graph traversal

```ppl
source=edges | where parent = "A" | fields parent, child
| union recursive name=rel max_depth=3 [
    source=edges as e
    | join right = r on e.parent = r.child rel as r
    | fields e.parent as parent, e.child as child
  ]
| sort parent, child
```

## Limitations

- Requires the Calcite engine. Set `plugins.calcite.enabled` to true.
- The anchor and recursive subsearch must return the same field names and types.
- Recursive relation names cannot be used as aliases inside the recursive block.
- Duplicates are preserved unless you remove them in the pipeline.
