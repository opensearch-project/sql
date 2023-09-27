/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import lombok.Data;

/** Index details in an async query. */
@Data
public class IndexDetails {
  private String indexName;
  private FullyQualifiedTableName fullyQualifiedTableName;
}
