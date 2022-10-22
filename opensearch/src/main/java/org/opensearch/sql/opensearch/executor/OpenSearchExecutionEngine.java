/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.executor;

import com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.planner.physical.SessionContext;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch execution engine implementation. */
@RequiredArgsConstructor
public class OpenSearchExecutionEngine implements ExecutionEngine {

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            /** TODO: Re-implment as  PhysicalPlan.next return a tuple of (ResultSet, Metadata
             * QueryStart time is introduced by a root now that returns
             *  (ResultSet.of(), MetaData[QueryStartTime]).
             *  Then Index scan can add _id and such to meta data.
             */
            List<ExprValue> result = new ArrayList<>();
            SessionContext currentContext = new ExecutionSessionContext(
                Clock.fixed(Instant.now(), ZoneId.systemDefault()), Clock.systemDefaultZone());
            plan.open(currentContext);

            while (plan.hasNext()) {
              result.add(plan.next());
            }

            QueryResponse response = new QueryResponse(physicalPlan.schema(), result);
            listener.onResponse(response);
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(() -> {
      try {
        Explain openSearchExplain = new Explain() {
          @Override
          public ExplainResponseNode visitTableScan(TableScanOperator node, Object context) {
            return explain(node, context, explainNode -> {
              explainNode.setDescription(ImmutableMap.of("request", node.explain()));
            });
          }
        };

        listener.onResponse(openSearchExplain.apply(plan));
      } catch (Exception e) {
        listener.onFailure(e);
      }
    });
  }

}
