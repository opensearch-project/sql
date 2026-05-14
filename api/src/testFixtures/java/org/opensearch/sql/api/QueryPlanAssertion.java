/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Mixin interface providing fluent assertion API for query plan verification. Tests can implement
 * this interface to gain access to {@link QueryAssert} and {@link QueryErrorAssert} for asserting
 * logical plan structure, field names, and operator return types.
 */
public interface QueryPlanAssertion {

  /** Fluent assertion on a query's logical plan. */
  @RequiredArgsConstructor
  class QueryAssert {
    private final RelNode plan;

    /** Assert the logical plan matches the expected tree string. */
    public QueryAssert assertPlan(String expected) {
      assertEquals(
          expected.stripTrailing(),
          RelOptUtil.toString(plan).replaceAll("\\r\\n", "\n").stripTrailing());
      return this;
    }

    /** Assert the logical plan contains the expected substring. */
    public QueryAssert assertPlanContains(String expected) {
      String planStr = RelOptUtil.toString(plan).replaceAll("\\r\\n", "\n");
      assertTrue(
          "Expected plan to contain: " + expected + "\nActual plan:\n" + planStr,
          planStr.contains(expected));
      return this;
    }

    /** Assert the output field names match. */
    public QueryAssert assertFields(String... names) {
      assertEquals(List.of(names), plan.getRowType().getFieldNames());
      return this;
    }

    /** Assert a function/operator in the plan has the expected return type. */
    public QueryAssert assertReturnType(String operatorName, SqlTypeName expectedType) {
      return assertReturnType(operatorName, expectedType, -1);
    }

    /** Assert a function/operator in the plan has the expected return type and precision. */
    public QueryAssert assertReturnType(
        String operatorName, SqlTypeName expectedType, int expectedPrecision) {
      RexCall call = findCall(operatorName);
      assertNotNull("No RexCall found for: " + operatorName, call);
      assertEquals(operatorName + " type", expectedType, call.getType().getSqlTypeName());
      if (expectedPrecision >= 0) {
        assertEquals(operatorName + " precision", expectedPrecision, call.getType().getPrecision());
      }
      return this;
    }

    /** Access the underlying plan for custom assertions. */
    public RelNode plan() {
      return plan;
    }

    private RexCall findCall(String operatorName) {
      AtomicReference<RexCall> ref = new AtomicReference<>();
      plan.accept(
          new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
              RelNode visited = super.visit(other);
              visited.accept(
                  new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                      if (ref.get() == null
                          && call.getOperator().getName().equalsIgnoreCase(operatorName)) {
                        ref.set(call);
                      }
                      return super.visitCall(call);
                    }
                  });
              return visited;
            }
          });
      return ref.get();
    }
  }

  /** Fluent assertion on a query planning error. */
  @RequiredArgsConstructor
  class QueryErrorAssert {
    private final Exception error;

    /** Assert the root cause error message contains the expected substring. */
    public QueryErrorAssert assertErrorMessage(String expected) {
      Throwable cause = error;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      String msg = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName();
      assertTrue(
          "Expected error to contain: " + expected + "\nActual: " + msg, msg.contains(expected));
      return this;
    }
  }
}
