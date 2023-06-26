/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Spark query request.
 */
@EqualsAndHashCode
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class SparkQueryRequest {

  /**
   * SQL.
   */
  private String sql;

}