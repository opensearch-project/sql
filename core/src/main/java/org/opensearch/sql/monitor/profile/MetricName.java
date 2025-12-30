/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Named metrics used by query profiling. */
public enum MetricName {
  ANALYZE_TIME,
  OPTIMIZE_TIME,
  OPENSEARCH_TIME,
  POST_EXEC_TIME,
  FORMAT_TIME
}
