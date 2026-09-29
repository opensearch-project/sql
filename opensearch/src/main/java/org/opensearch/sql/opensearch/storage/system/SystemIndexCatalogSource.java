/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.opensearch.sql.utils.SystemIndexUtils.systemTable;

import java.util.Map;
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
import org.opensearch.sql.utils.SystemIndexUtils;

/**
 * {@link CatalogSource} for the SHOW TABLES and DESCRIBE system tables, backed by index listing and
 * index field mappings.
 */
public class SystemIndexCatalogSource implements CatalogSource {

  private final Pair<OpenSearchSystemIndexSchema, OpenSearchSystemRequest> bundle;

  public SystemIndexCatalogSource(OpenSearchClient client, String indexName) {
    this.bundle = buildBundle(client, indexName);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return bundle.getLeft().getMapping();
  }

  @Override
  public OpenSearchSystemRequest createRequest() {
    return bundle.getRight();
  }

  @Override
  public PhysicalPlan implementV2(LogicalPlan plan) {
    return plan.accept(
        new DefaultImplementor<Object>() {
          @Override
          public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
            return new OpenSearchSystemIndexScan(bundle.getRight());
          }
        },
        null);
  }

  private static Pair<OpenSearchSystemIndexSchema, OpenSearchSystemRequest> buildBundle(
      OpenSearchClient client, String indexName) {
    SystemIndexUtils.SystemTable systemTable = systemTable(indexName);
    if (systemTable.isSystemInfoTable()) {
      return Pair.of(
          OpenSearchSystemIndexSchema.SYS_TABLE_TABLES, new OpenSearchCatIndicesRequest(client));
    } else {
      return Pair.of(
          OpenSearchSystemIndexSchema.SYS_TABLE_MAPPINGS,
          new OpenSearchDescribeIndexRequest(
              client, systemTable.getTableName(), systemTable.getLangSpec()));
    }
  }
}
