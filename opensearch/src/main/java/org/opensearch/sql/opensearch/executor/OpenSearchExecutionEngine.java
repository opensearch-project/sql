/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.executor;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.Explain;
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
            List<ExprValue> result = new ArrayList<>();
            plan.open();

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
