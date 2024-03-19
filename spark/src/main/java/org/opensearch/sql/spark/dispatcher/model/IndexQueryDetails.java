/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import static org.apache.commons.lang3.StringUtils.strip;

import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.spark.flint.FlintIndexType;

/** Index details in an async query. */
@Getter
@EqualsAndHashCode
public class IndexQueryDetails {

  public static final String STRIP_CHARS = "`";

  private static final Set<Character> INVALID_INDEX_NAME_CHARS =
      Set.of(' ', ',', ':', '"', '+', '/', '\\', '|', '?', '#', '>', '<');

  private String indexName;
  private FullyQualifiedTableName fullyQualifiedTableName;
  // by default, auto_refresh = false;
  private IndexQueryActionType indexQueryActionType;
  private FlintIndexOptions flintIndexOptions;
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

    public IndexQueryDetailsBuilder indexQueryActionType(
        IndexQueryActionType indexQueryActionType) {
      indexQueryDetails.indexQueryActionType = indexQueryActionType;
      return this;
    }

    public IndexQueryDetailsBuilder indexOptions(FlintIndexOptions flintIndexOptions) {
      indexQueryDetails.flintIndexOptions = flintIndexOptions;
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
      if (indexQueryDetails.flintIndexOptions == null) {
        indexQueryDetails.flintIndexOptions = new FlintIndexOptions();
      }
      return indexQueryDetails;
    }
  }

  public String openSearchIndexName() {
    FullyQualifiedTableName fullyQualifiedTableName = getFullyQualifiedTableName();
    String indexName = StringUtils.EMPTY;
    switch (getIndexType()) {
      case COVERING:
        indexName =
            "flint_"
                + fullyQualifiedTableName.toFlintName()
                + "_"
                + strip(getIndexName(), STRIP_CHARS)
                + "_"
                + getIndexType().getSuffix();
        break;
      case SKIPPING:
        indexName =
            "flint_" + fullyQualifiedTableName.toFlintName() + "_" + getIndexType().getSuffix();
        break;
      case MATERIALIZED_VIEW:
        indexName = "flint_" + new FullyQualifiedTableName(mvName).toFlintName();
        break;
    }
    return percentEncode(indexName).toLowerCase();
  }

  /*
   * Percent-encode invalid OpenSearch index name characters.
   */
  private String percentEncode(String indexName) {
    StringBuilder builder = new StringBuilder(indexName.length());
    for (char ch : indexName.toCharArray()) {
      if (INVALID_INDEX_NAME_CHARS.contains(ch)) {
        builder.append(String.format("%%%02X", (int) ch));
      } else {
        builder.append(ch);
      }
    }
    return builder.toString();
  }
}
