/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.datasource;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;

/** Definition of the data source table schema. */
@Getter
@RequiredArgsConstructor
public enum DataSourceTableSchema {
  DATASOURCE_TABLE_SCHEMA(
      new LinkedHashMap<>() {
        {
          put("DATASOURCE_NAME", STRING);
          put("CONNECTOR_TYPE", STRING);
        }
      });
  private final Map<String, ExprType> mapping;
}
