/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Named metrics used by query profiling. */
public enum MetricName {
  ANALYZE,
  OPTIMIZE,
  EXECUTE,
  FORMAT
}
