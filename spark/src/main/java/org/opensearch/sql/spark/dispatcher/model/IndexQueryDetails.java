/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.spark.flint.FlintIndexType;

/** Index details in an async query. */
@Getter
@EqualsAndHashCode
public class IndexQueryDetails {

  public static final String STRIP_CHARS = "`";

  private String indexName;
  private FullyQualifiedTableName fullyQualifiedTableName;
  // by default, auto_refresh = false;
  private boolean autoRefresh;
  private IndexQueryActionType indexQueryActionType;
  // materialized view special case where
  // table name and mv name are combined.
  private String mvName;
  private FlintIndexType indexType;

  private IndexQueryDetails() {}

  public static IndexQueryDetailsBuilder builder() {
    return new IndexQueryDetailsBuilder();
  }

  // Builder class
  public static class IndexQueryDetailsBuilder {
    private final IndexQueryDetails indexQueryDetails;

    public IndexQueryDetailsBuilder() {
      indexQueryDetails = new IndexQueryDetails();
    }

    public IndexQueryDetailsBuilder indexName(String indexName) {
      indexQueryDetails.indexName = indexName;
      return this;
    }

    public IndexQueryDetailsBuilder fullyQualifiedTableName(FullyQualifiedTableName tableName) {
      indexQueryDetails.fullyQualifiedTableName = tableName;
      return this;
    }

    public IndexQueryDetailsBuilder autoRefresh(Boolean autoRefresh) {
      indexQueryDetails.autoRefresh = autoRefresh;
      return this;
    }

    public IndexQueryDetailsBuilder indexQueryActionType(
        IndexQueryActionType indexQueryActionType) {
      indexQueryDetails.indexQueryActionType = indexQueryActionType;
      return this;
    }

    public IndexQueryDetailsBuilder mvName(String mvName) {
      indexQueryDetails.mvName = mvName;
      return this;
    }

    public IndexQueryDetailsBuilder indexType(FlintIndexType indexType) {
      indexQueryDetails.indexType = indexType;
      return this;
    }

    public IndexQueryDetails build() {
      return indexQueryDetails;
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
