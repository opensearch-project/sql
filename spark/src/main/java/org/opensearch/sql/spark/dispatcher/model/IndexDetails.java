/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.spark.flint.FlintIndexType;

/** Index details in an async query. */
@Getter
@EqualsAndHashCode
public class IndexDetails {

  public static final String STRIP_CHARS = "`";

  private String indexName;
  private FullyQualifiedTableName fullyQualifiedTableName;
  // by default, auto_refresh = false;
  private boolean autoRefresh;
  private boolean isDropIndex;
  // materialized view special case where
  // table name and mv name are combined.
  private String mvName;
  private FlintIndexType indexType;

  private IndexDetails() {}

  public static IndexDetailsBuilder builder() {
    return new IndexDetailsBuilder();
  }

  // Builder class
  public static class IndexDetailsBuilder {
    private final IndexDetails indexDetails;

    public IndexDetailsBuilder() {
      indexDetails = new IndexDetails();
    }

    public IndexDetailsBuilder indexName(String indexName) {
      indexDetails.indexName = indexName;
      return this;
    }

    public IndexDetailsBuilder fullyQualifiedTableName(FullyQualifiedTableName tableName) {
      indexDetails.fullyQualifiedTableName = tableName;
      return this;
    }

    public IndexDetailsBuilder autoRefresh(Boolean autoRefresh) {
      indexDetails.autoRefresh = autoRefresh;
      return this;
    }

    public IndexDetailsBuilder isDropIndex(boolean isDropIndex) {
      indexDetails.isDropIndex = isDropIndex;
      return this;
    }

    public IndexDetailsBuilder mvName(String mvName) {
      indexDetails.mvName = mvName;
      return this;
    }

    public IndexDetailsBuilder indexType(FlintIndexType indexType) {
      indexDetails.indexType = indexType;
      return this;
    }

    public IndexDetails build() {
      Preconditions.checkNotNull(indexDetails.indexType, "Index Type can't be null");
      switch (indexDetails.indexType) {
        case COVERING:
          Preconditions.checkNotNull(
              indexDetails.indexName, "IndexName can't be null for Covering Index.");
          Preconditions.checkNotNull(
              indexDetails.fullyQualifiedTableName, "TableName can't be null for Covering Index.");
          break;
        case SKIPPING:
          Preconditions.checkNotNull(
              indexDetails.fullyQualifiedTableName, "TableName can't be null for Skipping Index.");
          break;
        case MATERIALIZED_VIEW:
          Preconditions.checkNotNull(indexDetails.mvName, "Materialized view name can't be null");
          break;
      }

      return indexDetails;
    }
  }

  public String openSearchIndexName() {
    FullyQualifiedTableName fullyQualifiedTableName = getFullyQualifiedTableName();
    String indexName = StringUtils.EMPTY;
    switch (getIndexType()) {
      case COVERING:
        indexName =
            "flint"
                + "_"
                + StringUtils.strip(fullyQualifiedTableName.getDatasourceName(), STRIP_CHARS)
                + "_"
                + StringUtils.strip(fullyQualifiedTableName.getSchemaName(), STRIP_CHARS)
                + "_"
                + StringUtils.strip(fullyQualifiedTableName.getTableName(), STRIP_CHARS)
                + "_"
                + StringUtils.strip(getIndexName(), STRIP_CHARS)
                + "_"
                + getIndexType().getSuffix();
        break;
      case SKIPPING:
        indexName =
            "flint"
                + "_"
                + StringUtils.strip(fullyQualifiedTableName.getDatasourceName(), STRIP_CHARS)
                + "_"
                + StringUtils.strip(fullyQualifiedTableName.getSchemaName(), STRIP_CHARS)
                + "_"
                + StringUtils.strip(fullyQualifiedTableName.getTableName(), STRIP_CHARS)
                + "_"
                + getIndexType().getSuffix();
        break;
      case MATERIALIZED_VIEW:
        indexName = "flint" + "_" + StringUtils.strip(getMvName(), STRIP_CHARS).toLowerCase();
        break;
    }
    return indexName.toLowerCase();
  }
}
