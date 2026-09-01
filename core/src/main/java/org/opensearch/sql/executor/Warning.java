/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import lombok.Data;

/**
 * A non-fatal notice attached to an otherwise-successful query response. Carried through the
 * response path so consumers can distinguish a correct-but-noteworthy result (e.g. a partial result
 * over a subset of indices) from a plain success, without turning it into an error.
 */
@Data
public class Warning {

  /**
   * The result is complete for the indices it covers but omits one or more indices that could not
   * be served (e.g. a mapping conflict that prevents aggregation pushdown). This is a cross-surface
   * contract: consumers such as OpenSearch Dashboards branch on this {@code type} value, so it must
   * not change without coordinating those consumers.
   */
  public static final String TYPE_PARTIAL_RESULT = "PARTIAL_RESULT";

  /** Machine-readable category, e.g. {@link #TYPE_PARTIAL_RESULT}. */
  private final String type;

  /** Short human-readable summary. */
  private final String message;

  /** Optional longer explanation with the specifics and remedy; may be null. */
  private final String detail;
}
