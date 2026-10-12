/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import lombok.Data;

/**
 * A non-fatal notice attached to an otherwise-successful query response. Carried through the
 * response path so consumers can distinguish a correct-but-noteworthy result from a plain success,
 * without turning it into an error.
 */
@Data
public class Warning {

  /**
   * The response does not cover everything the query asked for. This is a cross-surface contract:
   * consumers such as OpenSearch Dashboards branch on this {@code type} value, so it must not
   * change without coordinating those consumers.
   */
  public static final String TYPE_PARTIAL_RESULT = "PARTIAL_RESULT";

  /** Machine-readable category, e.g. {@link #TYPE_PARTIAL_RESULT}. */
  private final String type;

  /** Short human-readable summary. */
  private final String message;

  /** Optional longer explanation with the specifics and remedy; may be null. */
  private final String detail;
}
