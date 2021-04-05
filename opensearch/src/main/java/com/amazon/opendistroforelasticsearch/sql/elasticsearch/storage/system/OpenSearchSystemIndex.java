/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.system;

import static com.amazon.opendistroforelasticsearch.sql.utils.SystemIndexUtils.systemTable;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.OpenSearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.system.OpenSearchCatIndicesRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.system.OpenSearchDescribeIndexRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.system.OpenSearchSystemRequest;
import com.amazon.opendistroforelasticsearch.sql.planner.DefaultImplementor;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalRelation;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.storage.Table;
import com.amazon.opendistroforelasticsearch.sql.utils.SystemIndexUtils;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Elasticsearch System Index Table Implementation.
 */
public class OpenSearchSystemIndex implements Table {
  /**
   * System Index Name.
   */
  private final Pair<OpenSearchSystemIndexSchema, OpenSearchSystemRequest> systemIndexBundle;

  public OpenSearchSystemIndex(
      OpenSearchClient client, String indexName) {
    this.systemIndexBundle = buildIndexBundle(client, indexName);
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
  public class OpenSearchSystemIndexDefaultImplementor
      extends DefaultImplementor<Object> {

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
      return Pair.of(OpenSearchSystemIndexSchema.SYS_TABLE_TABLES,
          new OpenSearchCatIndicesRequest(client));
    } else {
      return Pair.of(OpenSearchSystemIndexSchema.SYS_TABLE_MAPPINGS,
          new OpenSearchDescribeIndexRequest(client, systemTable.getTableName()));
    }
  }
}
