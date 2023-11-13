/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.opensearch.sql.spark.flint.FlintIndexType;

/** Index details in an async query. */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class IndexDetails {
  private String indexName;
  private FullyQualifiedTableName fullyQualifiedTableName;
  // by default, auto_refresh = false;
  private Boolean autoRefresh = false;
  private boolean isDropIndex;
  private FlintIndexType indexType;

  public String openSearchIndexName() {
    FullyQualifiedTableName fullyQualifiedTableName = getFullyQualifiedTableName();
    if (FlintIndexType.SKIPPING.equals(getIndexType())) {
      String indexName =
          "flint"
              + "_"
              + fullyQualifiedTableName.getDatasourceName()
              + "_"
              + fullyQualifiedTableName.getSchemaName()
              + "_"
              + fullyQualifiedTableName.getTableName()
              + "_"
              + getIndexType().getSuffix();
      return indexName.toLowerCase();
    } else if (FlintIndexType.COVERING.equals(getIndexType())) {
      String indexName =
          "flint"
              + "_"
              + fullyQualifiedTableName.getDatasourceName()
              + "_"
              + fullyQualifiedTableName.getSchemaName()
              + "_"
              + fullyQualifiedTableName.getTableName()
              + "_"
              + getIndexName()
              + "_"
              + getIndexType().getSuffix();
      return indexName.toLowerCase();
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported Index Type : %s", getIndexType()));
    }
  }
}
