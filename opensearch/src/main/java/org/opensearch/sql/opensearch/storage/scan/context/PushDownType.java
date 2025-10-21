/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

public enum PushDownType {
  FILTER,
  PROJECT,
  AGGREGATION,
  SORT,
  LIMIT,
  SCRIPT,
  COLLAPSE,
  SORT_AGG_METRICS
  // HIGHLIGHT,
  // NESTED
}
