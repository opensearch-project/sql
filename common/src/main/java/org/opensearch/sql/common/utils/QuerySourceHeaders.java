/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

/**
 * Constants for thread context headers used to propagate query source metadata to downstream
 * components like query-insights.
 */
public final class QuerySourceHeaders {

  public static final String QUERY_SOURCE_HEADER = "x-query-source";
  public static final String ORIGINAL_QUERY_HEADER = "x-original-query";
  public static final String QUERY_EXECUTION_ID_HEADER = "x-query-execution-id";
  public static final String QUERY_PHASES_HEADER = "x-query-phases";

  /** Maximum number of characters stored in the {@link #ORIGINAL_QUERY_HEADER}. */
  public static final int MAX_ORIGINAL_QUERY_LENGTH = 4096;

  private QuerySourceHeaders() {}
}
