/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;

/** Semantic Check Exception. */
public class SemanticCheckException extends QueryEngineException {

  /** Whether this error keeps a Calcite query from falling back to the V2 engine. */
  private boolean noCalciteFallback;

  public SemanticCheckException(String message) {
    super(message);
  }

  public SemanticCheckException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Mark this as an error of a feature that only the Calcite engine has, such as k-NN search and
   * vector columns. The V2 engine cannot answer such a query: falling back to it, which {@code
   * plugins.calcite.fallback.allowed} otherwise does for any Calcite error, would replace this
   * error with a misleading one or run the query with other semantics. A marked error is reported
   * as it is, and its type stays {@code SemanticCheckException}. Unmarked errors keep the existing
   * fallback behaviour.
   *
   * @return this exception
   */
  public SemanticCheckException withoutCalciteFallback() {
    this.noCalciteFallback = true;
    return this;
  }

  /** Whether {@link #withoutCalciteFallback()} marked this error. */
  public boolean isWithoutCalciteFallback() {
    return noCalciteFallback;
  }

  /**
   * Whether a Calcite failure must not fall back to the V2 engine: some throwable in its cause
   * chain is a {@link SemanticCheckException} marked by {@link #withoutCalciteFallback()}. The
   * chain is searched because errors raised while planning arrive wrapped, e.g. in an {@code
   * ErrorReport}.
   *
   * @param failure the failure of the Calcite engine
   */
  public static boolean preventsCalciteFallback(Throwable failure) {
    return ExceptionUtils.getThrowableList(failure).stream()
        .anyMatch(t -> t instanceof SemanticCheckException e && e.isWithoutCalciteFallback());
  }
}
