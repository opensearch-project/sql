/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.s3.storage;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

@RequiredArgsConstructor
public class S3Table implements Table {

  private static final Map<String, ExprType> S3_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("text", OpenSearchDataType.OPENSEARCH_TEXT)
          .put("string", OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD)
          .put("text_keyword", OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD)
          .put("keyword", ExprCoreType.STRING)
          .put("byte", ExprCoreType.BYTE)
          .put("short", ExprCoreType.SHORT)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("half_float", ExprCoreType.FLOAT)
          .put("scaled_float", ExprCoreType.DOUBLE)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("date_nanos", ExprCoreType.TIMESTAMP)
          .put("ip", OpenSearchDataType.OPENSEARCH_IP)
          .put("geo_point", OpenSearchDataType.OPENSEARCH_GEO_POINT)
          .put("binary", OpenSearchDataType.OPENSEARCH_BINARY)
          .build();

  private final String tableName;

  private final Map<String, String> colNameTypes;

  private final String fileFormat;

  private final String location;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    for (Map.Entry<String, String> entry : colNameTypes.entrySet()) {
      fieldTypes.put(
          entry.getKey(), S3_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(entry.getValue(), UNKNOWN));
    }
    return fieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new S3PlanImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class S3PlanImplementor extends DefaultImplementor<Void> {
    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Void context) {
      return new S3ScanOperator(location);
    }
  }
}
