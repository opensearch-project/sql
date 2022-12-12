/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.planner.physical.ADOperator;
import org.opensearch.sql.opensearch.planner.physical.MLCommonsOperator;
import org.opensearch.sql.opensearch.planner.physical.MLOperator;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanBuilder;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalML;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/** OpenSearch table (index) implementation. */
public class OpenSearchIndex implements Table {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  private final Settings settings;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final OpenSearchRequest.IndexName indexName;

  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * The cached max result window setting of index.
   */
  private Integer cachedMaxResultWindow = null;

  /**
   * Constructor.
   */
  public OpenSearchIndex(OpenSearchClient client, Settings settings, String indexName) {
    this.client = client;
    this.settings = settings;
    this.indexName = new OpenSearchRequest.IndexName(indexName);
  }

  @Override
  public boolean exists() {
    return client.exists(indexName.toString());
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    mappings.put("properties", properties);

    for (Map.Entry<String, ExprType> colType : schema.entrySet()) {
      properties.put(colType.getKey(), OpenSearchDataType.getOpenSearchType(colType.getValue()));
    }
    client.createIndex(indexName.toString(), mappings);
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes = new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * Get the max result window setting of the table.
   */
  public Integer getMaxResultWindow() {
    if (cachedMaxResultWindow == null) {
      cachedMaxResultWindow =
          new OpenSearchDescribeIndexRequest(client, indexName).getMaxResultWindow();
    }
    return cachedMaxResultWindow;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    // TODO: Leave it here to avoid impact Prometheus and AD operators. Need to move to Planner.
    return plan.accept(new OpenSearchDefaultImplementor(client), null);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    // No-op because optimization already done in Planner
    return plan;
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client, settings, indexName,
        getMaxResultWindow(), new OpenSearchExprValueFactory(getFieldTypes()));
    return new OpenSearchIndexScanBuilder(indexScan);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class OpenSearchDefaultImplementor
      extends DefaultImplementor<OpenSearchIndexScan> {

    private final OpenSearchClient client;

    @Override
    public PhysicalPlan visitMLCommons(LogicalMLCommons node, OpenSearchIndexScan context) {
      return new MLCommonsOperator(visitChild(node, context), node.getAlgorithm(),
              node.getArguments(), client.getNodeClient());
    }

    @Override
    public PhysicalPlan visitAD(LogicalAD node, OpenSearchIndexScan context) {
      return new ADOperator(visitChild(node, context),
              node.getArguments(), client.getNodeClient());
    }

    @Override
    public PhysicalPlan visitML(LogicalML node, OpenSearchIndexScan context) {
      return new MLOperator(visitChild(node, context),
              node.getArguments(), client.getNodeClient());
    }
  }
}
