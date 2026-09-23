/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.pagination;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.executor.pagination.serde.IndexScanNode;
import org.opensearch.sql.executor.pagination.serde.ProjectNode;
import org.opensearch.sql.executor.pagination.serde.SerializablePlanNode;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.serde.DefaultExpressionSerializer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.StorageEngine;

/** Rebuilds physical plans from the closed cursor schema. */
public final class PlanRebuilder implements PlanSerializer.PlanRebuildFunction {

  /** Public no-arg constructor required by ServiceLoader. */
  public PlanRebuilder() {}

  @Override
  public PhysicalPlan rebuild(SerializablePlanNode node, StorageEngine storageEngine) {
    if (!(storageEngine instanceof OpenSearchStorageEngine osEngine)) {
      throw new IllegalStateException(
          "Cannot rebuild cursor: storage engine is not OpenSearchStorageEngine");
    }
    return rebuildNode(node, osEngine);
  }

  private PhysicalPlan rebuildNode(
      SerializablePlanNode node, OpenSearchStorageEngine storageEngine) {
    return switch (node) {
      case ProjectNode projectNode -> rebuildProject(projectNode, storageEngine);
      case IndexScanNode indexScanNode -> rebuildIndexScan(indexScanNode, storageEngine);
    };
  }

  private ProjectOperator rebuildProject(
      ProjectNode projectNode, OpenSearchStorageEngine storageEngine) {
    DefaultExpressionSerializer expressionSerializer =
        new DefaultExpressionSerializer(storageEngine::getSettings);
    List<NamedExpression> projectList =
        projectNode.projectList().stream()
            .map(code -> (NamedExpression) expressionSerializer.deserialize(code))
            .collect(Collectors.toList());

    PhysicalPlan child = rebuildNode(projectNode.child(), storageEngine);
    return new ProjectOperator(child, projectList, List.of());
  }

  private OpenSearchIndexScan rebuildIndexScan(
      IndexScanNode indexScanNode, OpenSearchStorageEngine storageEngine) {
    try (BytesStreamInput bsi = new BytesStreamInput(indexScanNode.requestBytes())) {
      OpenSearchQueryRequest request = new OpenSearchQueryRequest(bsi, storageEngine);
      return new OpenSearchIndexScan(
          storageEngine.getClient(), indexScanNode.maxResponseSize(), request);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize OpenSearchQueryRequest", e);
    }
  }
}
