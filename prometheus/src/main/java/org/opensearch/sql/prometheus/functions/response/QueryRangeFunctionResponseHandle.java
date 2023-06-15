/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.functions.response;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Handle Prometheus response.
 */
public interface QueryRangeFunctionResponseHandle {

  /**
   * Return true if Prometheus  response has more result.
   */
  boolean hasNext();

  /**
   * Return Prometheus response as {@link ExprValue}. Attention, the method must been called when
   * hasNext return true.
   */
  ExprValue next();

  /**
   * Return ExecutionEngine.Schema of the Prometheus response.
   */
  ExecutionEngine.Schema schema();
}