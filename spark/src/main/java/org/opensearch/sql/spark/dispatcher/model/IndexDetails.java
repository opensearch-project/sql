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
}
