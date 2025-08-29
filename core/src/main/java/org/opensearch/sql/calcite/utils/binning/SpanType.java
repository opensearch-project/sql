/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

/** Enumeration of different span types for bin operations. */
public enum SpanType {
  /** Logarithmic span (e.g., log10, 2log10) */
  LOG,

  /** Time-based span (e.g., 30seconds, 15minutes) */
  TIME,

  /** Numeric span (e.g., 100, 25.5) */
  NUMERIC
}
