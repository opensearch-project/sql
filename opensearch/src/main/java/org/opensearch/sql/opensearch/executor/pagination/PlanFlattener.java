/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.pagination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.executor.pagination.PlanSerializer.FlattenResult;
import org.opensearch.sql.executor.pagination.serde.IndexScanNode;
import org.opensearch.sql.executor.pagination.serde.ProjectNode;
import org.opensearch.sql.executor.pagination.serde.SerializablePlanNode;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.executor.protector.ResourceMonitorPlan;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.serde.DefaultExpressionSerializer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;

/** Flattens supported physical plans into the closed cursor serialization schema. */
public final class PlanFlattener implements PlanSerializer.PlanFlattenFunction {

  /** Public no-arg constructor required by ServiceLoader. */
  public PlanFlattener() {}

  @Override
  public FlattenResult flatten(PhysicalPlan plan, Settings settings) throws NoCursorException {
    DefaultExpressionSerializer expressionSerializer =
        settings == null
            ? new DefaultExpressionSerializer()
            : new DefaultExpressionSerializer(() -> settings);
    List<OpenSearchIndexScan> serializedScans = new ArrayList<>();
    List<String> serializedExpressions = new ArrayList<>();
    SerializablePlanNode node =
        flattenNode(plan, serializedScans, serializedExpressions, expressionSerializer);
    List<OpenSearchIndexScan> committedScans = List.copyOf(serializedScans);
    List<String> committedExpressions = List.copyOf(serializedExpressions);
    return new FlattenResult(
        node,
        () -> {
          committedExpressions.forEach(expressionSerializer::deserialize);
          committedScans.forEach(OpenSearchIndexScan::markCursorSerialized);
        });
  }

  private SerializablePlanNode flattenNode(
      PhysicalPlan plan,
      List<OpenSearchIndexScan> serializedScans,
      List<String> serializedExpressions,
      DefaultExpressionSerializer expressionSerializer) {
    if (plan instanceof ResourceMonitorPlan resourceMonitorPlan) {
      return flattenNode(
          resourceMonitorPlan.getDelegate(),
          serializedScans,
          serializedExpressions,
          expressionSerializer);
    }
    if (plan instanceof ProjectOperator projectOp) {
      return flattenProject(
          projectOp, serializedScans, serializedExpressions, expressionSerializer);
    }
    if (plan instanceof OpenSearchIndexScan indexScan) {
      return flattenIndexScan(indexScan, serializedScans);
    }
    throw new NoCursorException();
  }

  private ProjectNode flattenProject(
      ProjectOperator projectOp,
      List<OpenSearchIndexScan> serializedScans,
      List<String> serializedExpressions,
      DefaultExpressionSerializer expressionSerializer) {
    List<String> serializedExprs =
        projectOp.getProjectList().stream()
            .map(expr -> expressionSerializer.serialize((NamedExpression) expr))
            .toList();
    serializedExpressions.addAll(serializedExprs);

    PhysicalPlan child = projectOp.getChild().get(0);
    SerializablePlanNode childNode =
        flattenNode(child, serializedScans, serializedExpressions, expressionSerializer);
    return new ProjectNode(serializedExprs, childNode);
  }

  private IndexScanNode flattenIndexScan(
      OpenSearchIndexScan indexScan, List<OpenSearchIndexScan> serializedScans) {
    if (!indexScan.getRequest().hasAnotherBatch()) {
      throw new NoCursorException();
    }
    try (BytesStreamOutput reqOut = new BytesStreamOutput()) {
      indexScan.getRequest().writeTo(reqOut);
      reqOut.flush();
      IndexScanNode node =
          new IndexScanNode(BytesReference.toBytes(reqOut.bytes()), indexScan.getMaxResponseSize());
      serializedScans.add(indexScan);
      return node;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize OpenSearchIndexScan request", e);
    }
  }
}
