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
  /** Machine-readable category, e.g. {@code PARTIAL_RESULT}. */
  private final String type;

  /** Short human-readable summary. */
  private final String message;

  /** Optional longer explanation with the specifics and remedy; may be null. */
  private final String detail;
}
