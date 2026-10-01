/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import com.alibaba.druid.sql.parser.ParserException;
import java.sql.SQLFeatureNotSupportedException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.legacy.antlr.SqlAnalysisException;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;

/** Utility to classify exceptions as client errors (4xx) vs server errors (5xx). */
public final class ErrorClassifier {

  private ErrorClassifier() {}

  /**
   * Returns true if the exception represents a client error (bad query, unsupported feature, etc.)
   * that should return HTTP 4xx, not 5xx.
   */
  public static boolean isClientError(Exception e) {
    return isClientErrorType(e)
        || (e instanceof RuntimeException
            && e.getCause() != null
            && isClientErrorType(e.getCause()));
  }

  /** Returns true if the throwable is a known client error type. */
  public static boolean isClientErrorType(Throwable t) {
    return t instanceof SqlParseException
        || t instanceof ParserException
        || t instanceof SQLFeatureNotSupportedException
        || t instanceof SQLFeatureDisabledException
        || t instanceof IllegalArgumentException
        || t instanceof UnsupportedOperationException
        || t instanceof IndexNotFoundException
        || t instanceof VerificationException
        || t instanceof SqlAnalysisException
        || t instanceof SyntaxCheckException
        || t instanceof SemanticCheckException
        || t instanceof ExpressionEvaluationException;
  }
}
