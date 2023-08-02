/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

/** Exception for Expression Evaluation. */
public class ExpressionEvaluationException extends QueryEngineException {
  public ExpressionEvaluationException(String message) {
    super(message);
  }
}
