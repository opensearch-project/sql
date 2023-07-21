/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.request;

import lombok.Data;

/**
 * Spark query request.
 */
@Data
public class SparkQueryRequest {

  /**
   * SQL.
   */
  private String sql;

}
