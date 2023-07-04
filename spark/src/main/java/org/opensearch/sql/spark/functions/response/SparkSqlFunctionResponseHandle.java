/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.response;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Handle Spark response.
 */
public interface SparkSqlFunctionResponseHandle {

  /**
   * Return true if Spark response has more result.
   */
  boolean hasNext();

  /**
   * Return Spark response as {@link ExprValue}. Attention, the method must been called when
   * hasNext return true.
   */
  ExprValue next();

  /**
   * Return ExecutionEngine.Schema of the Spark response.
   */
  ExecutionEngine.Schema schema();
}