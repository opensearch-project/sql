/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

/** Push down types. */
public enum PushDownType {
  FILTER,
  PROJECT,
  AGGREGATION,
  SORT,
  LIMIT,
  SCRIPT, // script in predicate
  SORT_AGG_METRICS, // convert composite aggregate to terms or multi-terms bucket aggregate
  RARE_TOP, // convert composite aggregate to nested aggregate
  SORT_EXPR,
  HIGHLIGHT
  // NESTED
}
