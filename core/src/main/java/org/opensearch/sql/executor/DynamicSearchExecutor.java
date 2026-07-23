/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexSubQuery;
import org.opensearch.sql.calcite.DynamicSearchPlanBinder;
import org.opensearch.sql.calcite.SearchPredicateCompiler;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;

/** Executes and binds implicit format subqueries before running the parent search. */
public final class DynamicSearchExecutor {

  private DynamicSearchExecutor() {}

  @FunctionalInterface
  public interface PlanExecutor {
    void execute(RelNode plan, ResponseListener<ExecutionEngine.QueryResponse> listener);
  }

  public static void execute(
      RelNode plan,
      SearchPredicateCompiler compiler,
      PlanExecutor subqueryExecutor,
      PlanExecutor finalExecutor,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    bind(
        plan,
        compiler,
        subqueryExecutor,
        new ResponseListener<>() {
          @Override
          public void onResponse(RelNode bound) {
            finalExecutor.execute(bound, listener);
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }

  /** Executes all implicit format subqueries and returns a plan with literal search predicates. */
  public static void bind(
      RelNode plan,
      SearchPredicateCompiler compiler,
      PlanExecutor subqueryExecutor,
      ResponseListener<RelNode> listener) {
    var dynamic = DynamicSearchPlanBinder.find(plan);
    if (dynamic.isEmpty()) {
      listener.onResponse(plan);
      return;
    }

    RexSubQuery subquery = dynamic.get();
    subqueryExecutor.execute(
        subquery.rel,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            try {
              String formatted = extractScalarString(response);
              String compiled = compiler.compile(formatted);
              RelNode bound = DynamicSearchPlanBinder.bind(plan, subquery, compiled);
              bind(bound, compiler, subqueryExecutor, listener);
            } catch (Exception e) {
              listener.onFailure(e);
            }
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }

  private static String extractScalarString(ExecutionEngine.QueryResponse response) {
    List<ExprValue> rows = response.getResults();
    if (rows.size() != 1 || rows.getFirst().tupleValue().size() != 1) {
      throw new SemanticCheckException(
          "Implicit format subsearch must return exactly one row and one column");
    }
    ExprValue value = rows.getFirst().tupleValue().values().iterator().next();
    if (value.isNull() || value.isMissing()) {
      throw new SemanticCheckException("Implicit format subsearch returned a null search string");
    }
    Object raw = value.value();
    if (!(raw instanceof String string)) {
      throw new SemanticCheckException("Implicit format subsearch must return a string");
    }
    return string;
  }
}
