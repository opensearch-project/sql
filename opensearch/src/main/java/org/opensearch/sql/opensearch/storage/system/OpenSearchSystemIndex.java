/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.opensearch.sql.utils.SystemIndexUtils.systemTable;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.system.OpenSearchCatIndicesRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;

/** OpenSearch System Index Table Implementation. */
public class OpenSearchSystemIndex implements Table {
  /** System Index Name. */
  private final Pair<OpenSearchSystemIndexSchema, OpenSearchSystemRequest> systemIndexBundle;

  public OpenSearchSystemIndex(OpenSearchClient client, String indexName) {
    this.systemIndexBundle = buildIndexBundle(client, indexName);
  }

  @Override
  public boolean exists() {
    return true; // TODO: implement for system index later
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "OpenSearch system index is predefined and cannot be created");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return systemIndexBundle.getLeft().getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new OpenSearchSystemIndexDefaultImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class OpenSearchSystemIndexDefaultImplementor extends DefaultImplementor<Object> {

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new OpenSearchSystemIndexScan(systemIndexBundle.getRight());
    }
  }

  /**
   * Constructor of ElasticsearchSystemIndexName.
   *
   * @param indexName index name;
   */
  private Pair<OpenSearchSystemIndexSchema, OpenSearchSystemRequest> buildIndexBundle(
      OpenSearchClient client, String indexName) {
    SystemIndexUtils.SystemTable systemTable = systemTable(indexName);
    if (systemTable.isSystemInfoTable()) {
      return Pair.of(
          OpenSearchSystemIndexSchema.SYS_TABLE_TABLES, new OpenSearchCatIndicesRequest(client));
    } else {
      return Pair.of(
          OpenSearchSystemIndexSchema.SYS_TABLE_MAPPINGS,
          new OpenSearchDescribeIndexRequest(client, systemTable.getTableName()));
    }
  }
}
