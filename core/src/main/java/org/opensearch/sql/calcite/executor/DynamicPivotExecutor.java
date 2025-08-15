/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.DynamicPivotOperator;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * DynamicPivotExecutor is responsible for executing the two-phase approach for the timechart
 * command. It first executes a query to get distinct values for the "by" field, then uses those
 * values to build the pivot table.
 */
public class DynamicPivotExecutor {

  /**
   * Execute the pivot operation in two phases: 1. Execute a query to get distinct values for the
   * "by" field 2. Use those distinct values to build the pivot table
   *
   * @param executionEngine The execution engine to use for executing the queries
   * @param relBuilder The RelBuilder to use for building the pivot table
   * @param timeField The name of the time field
   * @param byField The name of the "by" field
   * @param valueField The name of the value field
   * @param distinctQuery The query to get distinct values for the "by" field
   * @param pivotQuery The query to build the pivot table
   * @param listener The response listener to call back with the result
   */
  public static void execute(
      ExecutionEngine executionEngine,
      RelBuilder relBuilder,
      String timeField,
      String byField,
      String valueField,
      PhysicalPlan distinctQuery,
      PhysicalPlan pivotQuery,
      ResponseListener<QueryResponse> listener) {

    // Execute the distinct query to get distinct values for the "by" field
    CompletableFuture<QueryResponse> distinctResponseFuture = new CompletableFuture<>();
    executionEngine.execute(
        distinctQuery,
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            distinctResponseFuture.complete(response);
          }

          @Override
          public void onFailure(Exception e) {
            distinctResponseFuture.completeExceptionally(e);
            listener.onFailure(e);
          }
        });

    try {
      QueryResponse distinctResponse = distinctResponseFuture.get();
      List<ExprValue> distinctResult = distinctResponse.getResults();

      // Get the distinct values for the "by" field
      List<String> distinctValues =
          distinctResult.stream()
              .map(exprValue -> exprValue.tupleValue().get(byField).stringValue())
              .collect(Collectors.toList());

      // Build the pivot table using the distinct values
      RelNode pivotNode =
          DynamicPivotOperator.executePivot(
              relBuilder, timeField, byField, valueField, distinctValues);

      // Execute the pivot query
      executionEngine.execute(pivotQuery, listener);
    } catch (InterruptedException | ExecutionException e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the pivot operation in two phases: 1. Execute a query to get distinct values for the
   * "by" field 2. Use those distinct values to build the pivot table
   *
   * @param executionEngine The execution engine to use for executing the queries
   * @param relBuilder The RelBuilder to use for building the pivot table
   * @param timeField The name of the time field
   * @param byField The name of the "by" field
   * @param valueField The name of the value field
   * @param distinctQuery The query to get distinct values for the "by" field
   * @param pivotQuery The query to build the pivot table
   * @param listener The response listener to call back with the explanation
   */
  public static void explain(
      ExecutionEngine executionEngine,
      RelBuilder relBuilder,
      String timeField,
      String byField,
      String valueField,
      PhysicalPlan distinctQuery,
      PhysicalPlan pivotQuery,
      ResponseListener<ExplainResponse> listener) {

    // Explain the distinct query
    CompletableFuture<ExplainResponse> distinctExplainFuture = new CompletableFuture<>();
    executionEngine.explain(
        distinctQuery,
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            distinctExplainFuture.complete(response);
          }

          @Override
          public void onFailure(Exception e) {
            distinctExplainFuture.completeExceptionally(e);
            listener.onFailure(e);
          }
        });

    // Explain the pivot query
    CompletableFuture<ExplainResponse> pivotExplainFuture = new CompletableFuture<>();
    executionEngine.explain(
        pivotQuery,
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            pivotExplainFuture.complete(response);
          }

          @Override
          public void onFailure(Exception e) {
            pivotExplainFuture.completeExceptionally(e);
            listener.onFailure(e);
          }
        });

    try {
      ExplainResponse distinctExplain = distinctExplainFuture.get();
      ExplainResponse pivotExplain = pivotExplainFuture.get();

      // Combine the explanations
      ExplainResponseNode root = new ExplainResponseNode("DynamicPivot");
      Map<String, Object> description = new HashMap<>();
      description.put("timeField", timeField);
      description.put("byField", byField);
      description.put("valueField", valueField);
      root.setDescription(description);

      List<ExplainResponseNode> children = new ArrayList<>();
      if (distinctExplain.getRoot() != null) {
        children.add(distinctExplain.getRoot());
      }
      if (pivotExplain.getRoot() != null) {
        children.add(pivotExplain.getRoot());
      }
      root.setChildren(children);

      listener.onResponse(new ExplainResponse(root));
    } catch (InterruptedException | ExecutionException e) {
      listener.onFailure(e);
    }
  }
}
