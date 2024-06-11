/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;

/**
 * Abstraction over the IndexDMLResult storage. It stores the result of IndexDML query execution.
 */
public interface IndexDMLResultStorageService {
  IndexDMLResult createIndexDMLResult(
      IndexDMLResult result, AsyncQueryRequestContext asyncQueryRequestContext);
}
