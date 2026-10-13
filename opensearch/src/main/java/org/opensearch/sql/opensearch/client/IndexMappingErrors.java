/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;

/** Errors from reading index mappings, the same for local and remote indices. */
final class IndexMappingErrors {

  static final String LOCATION = "while fetching index mappings";

  private IndexMappingErrors() {}

  /** Re-thrown directly to be treated as a client error. */
  static ErrorReport indexNotFound(IndexNotFoundException e, String indexName) {
    return ErrorReport.wrap(e)
        .code(ErrorCode.INDEX_NOT_FOUND)
        .location(LOCATION)
        .context("index_name", indexName)
        .build();
  }

  /** Every remote cluster in the expression was unreachable and has skip_unavailable set. */
  static ErrorReport allClustersSkipped(String message, String indexName) {
    return ErrorReport.wrap(new OpenSearchStatusException(message, RestStatus.SERVICE_UNAVAILABLE))
        .code(ErrorCode.EXECUTION_ERROR)
        .location(LOCATION)
        .context("index_name", indexName)
        .build();
  }

  static ErrorReport permissionDenied(OpenSearchSecurityException e, String indexName) {
    return ErrorReport.wrap(e)
        .code(ErrorCode.PERMISSION_DENIED)
        .location(LOCATION)
        .context("index_name", indexName)
        .build();
  }
}
