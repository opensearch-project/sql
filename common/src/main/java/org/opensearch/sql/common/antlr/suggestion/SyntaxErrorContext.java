/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

/** Context passed to each {@link SyntaxErrorSuggestionProvider} for pattern matching. */
@RequiredArgsConstructor
@Getter
public class SyntaxErrorContext {
  private final Recognizer<?, ?> recognizer;
  private final Token offendingToken;
  private final CommonTokenStream tokens;
  private final String query;
  private final RecognitionException exception;

  public String getOffendingText() {
    return offendingToken == null ? "" : offendingToken.getText();
  }

  /** Text of the query after the offending token (trimmed). */
  public String getRemainingQuery() {
    if (offendingToken == null) return "";
    int end = offendingToken.getStopIndex() + 1;
    return end >= query.length() ? "" : query.substring(end);
  }

  public List<Token> getAllTokens() {
    return tokens.getTokens();
  }
}
