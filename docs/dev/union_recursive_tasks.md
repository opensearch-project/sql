# UNION RECURSIVE in PPL - High-Level Task Breakdown

This document captures the implementation plan for supporting `UNION RECURSIVE` in OpenSearch PPL.

## 1) Add syntax/grammar support
- Parse `UNION RECURSIVE name=<identifier> [max_depth=<N>] [max_rows=<M>] [ <subpipeline> ]`.
- Treat the recursive subpipeline as a bracketed pipeline node in the AST.
- Extend existing UNION grammar/entrypoints to support the recursive variant.
- Add parser tests for valid/invalid forms (missing name, missing subpipeline, invalid bounds).

## 2) Extend AST and logical plan nodes
- Add/extend AST nodes to represent anchor pipeline, recursive subpipeline, and options.
- Carry the recursive relation name so it can be resolved inside the subpipeline.
- Add logical plan nodes that map cleanly to Calcite recursive operators (e.g., `RepeatUnion`).
- Validate schema alignment between anchor and recursive outputs.

## 3) Implement analyzer/resolver rules
- Bind the recursive relation name inside the recursive subpipeline scope.
- Enforce scoping rules (recursive name visible only inside the bracketed block).
- Validate naming conflicts and prevent shadowing by other relations.

## 4) Add Calcite translation
- Translate anchor + recursive block into Calcite recursive relational algebra.
- Ensure anchor is the base and recursive block references the recursive relation.
- Preserve bag semantics (`UNION ALL`) and unioned output.
- Wire through optional limits (max_depth, max_rows) as planner hints or runtime controls.

## 5) Enforce runtime safety controls
- Add execution-time guards for max_depth and max_rows.
- Decide defaults and behavior when limits are hit (truncate vs error).
- Add tests for fixpoint termination and limits.

## 6) Tests and documentation
- Add parser/planner tests for syntax and plan generation.
- Add execution tests for common patterns (BoM, tree traversal, reachability).
- Update PPL user docs with syntax, examples, and limitations.
- Add negative tests for schema mismatch between anchor and recursive block.
