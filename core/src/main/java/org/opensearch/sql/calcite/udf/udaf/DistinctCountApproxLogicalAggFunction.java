/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * Logical marker UDAF for {@code DISTINCT_COUNT_APPROX}. Lets the PPL parser produce a RelNode that
 * contains the operator without committing to a JVM execution path; backends are expected to push
 * it down or rewrite it before execution:
 *
 * <ul>
 *   <li>OpenSearch V3 path: {@code OpenSearchExecutionEngine#registerOpenSearchFunctions} registers
 *       a real HyperLogLog++ implementation in {@code PPLFuncImpTable.aggExternalFunctionRegistry},
 *       which overrides this marker (external registry has lookup precedence in {@code
 *       getImplementation}). {@code AggregateAnalyzer} then translates the operator to OpenSearch
 *       cardinality DSL.
 *   <li>Unified-query / DataFusion / analytics-engine path: backend planner rewrites the RexOver to
 *       {@code APPROX_COUNT_DISTINCT} (Calcite stdop) before substrait emission; the DataFusion
 *       substrait reader consumes that natively.
 * </ul>
 *
 * <p>This class deliberately throws on every method. Reaching a method body means a backend either
 * failed to push down or did not register an adapter — that is a configuration bug, not a runtime
 * fallback. {@code RelevanceQueryFunction.RelevanceQueryImplementor} (used by {@code match}, {@code
 * match_phrase}, etc.) follows the same pattern for relevance search functions that have no JVM
 * execution semantics.
 */
public class DistinctCountApproxLogicalAggFunction
    implements UserDefinedAggFunction<DistinctCountApproxLogicalAggFunction.MarkerAccumulator> {

  private static final String NOT_EXECUTABLE =
      "DISTINCT_COUNT_APPROX logical marker reached Enumerable execution; "
          + "an engine-specific implementation must be registered or rewritten before execution.";

  @Override
  public MarkerAccumulator init() {
    throw new UnsupportedOperationException(NOT_EXECUTABLE);
  }

  @Override
  public MarkerAccumulator add(MarkerAccumulator acc, Object... values) {
    throw new UnsupportedOperationException(NOT_EXECUTABLE);
  }

  @Override
  public Object result(MarkerAccumulator accumulator) {
    throw new UnsupportedOperationException(NOT_EXECUTABLE);
  }

  /** Placeholder accumulator. Never actually constructed because {@link #init()} throws. */
  public static class MarkerAccumulator implements Accumulator {
    @Override
    public Object value(Object... argList) {
      throw new UnsupportedOperationException(NOT_EXECUTABLE);
    }
  }
}
