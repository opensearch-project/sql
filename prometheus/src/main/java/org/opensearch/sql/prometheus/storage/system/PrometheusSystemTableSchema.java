/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.system;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;

@Getter
@RequiredArgsConstructor
public enum PrometheusSystemTableSchema {
  SYS_TABLE_TABLES(
      new ImmutableMap.Builder<String, ExprType>()
          .put("TABLE_CATALOG", STRING)
          .put("TABLE_SCHEMA", STRING)
          .put("TABLE_NAME", STRING)
          .put("TABLE_TYPE", STRING)
          .put("UNIT", STRING)
          .put("REMARKS", STRING)
          .build()),
  SYS_TABLE_MAPPINGS(
      new ImmutableMap.Builder<String, ExprType>()
          .put("TABLE_CATALOG", STRING)
          .put("TABLE_SCHEMA", STRING)
          .put("TABLE_NAME", STRING)
          .put("COLUMN_NAME", STRING)
          .put("DATA_TYPE", STRING)
          .build());

  private final Map<String, ExprType> mapping;
}
