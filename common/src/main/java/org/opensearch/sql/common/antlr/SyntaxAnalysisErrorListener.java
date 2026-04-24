/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.opensearch.sql.common.antlr.suggestion.SyntaxErrorContext;
import org.opensearch.sql.common.antlr.suggestion.SyntaxErrorSuggestionRegistry;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;

/**
 * Standardized syntax-error listener. Wraps every ANTLR syntax error in an {@link ErrorReport} with
 * a fixed code, stage, location, and context schema so downstream consumers can rely on a single
 * shape.
 */
public class SyntaxAnalysisErrorListener extends BaseErrorListener {
  private static final int CONTEXT_TRUNCATION_THRESHOLD = 20;
  private static final int EXPECTED_TOKENS_THRESHOLD = 5;
  private static final String LOCATION = "while parsing query syntax";

  @Override
  public void syntaxError(
      Recognizer<?, ?> recognizer,
      Object offendingSymbol,
      int line,
      int charPositionInLine,
      String msg,
      RecognitionException e) {

    CommonTokenStream tokens = (CommonTokenStream) recognizer.getInputStream();
    Token offendingToken = (Token) offendingSymbol;
    String query = tokens.getText();

    String offendingText = offendingToken != null ? offendingToken.getText() : "<unknown>";
    String queryContext = truncateQueryAtOffendingToken(query, offendingToken);
    String details =
        String.format(
            Locale.ROOT,
            "[%s] is not a valid term at this part of the query: '%s' <-- HERE. %s",
            offendingText,
            queryContext,
            getDetails(recognizer, msg, e));

    List<String> suggestions;
    try {
      suggestions =
          SyntaxErrorSuggestionRegistry.findSuggestions(
              new SyntaxErrorContext(recognizer, offendingToken, tokens, query, e));
    } catch (Exception suggestionFailure) {
      // Never let a buggy provider turn a 400 syntax error into a 500.
      suggestions = List.of();
    }

    SyntaxCheckException cause = new SyntaxCheckException(details);
    throw ErrorReport.wrap(cause)
        .code(ErrorCode.SYNTAX_ERROR)
        .stage(QueryProcessingStage.ANALYZING)
        .location(LOCATION)
        .details(details)
        .context("offending_token", offendingText)
        .context("line", line)
        .context("column", charPositionInLine)
        .context("query_context", queryContext)
        .context("suggestions", suggestions)
        .build();
  }

  private static String truncateQueryAtOffendingToken(String query, Token offendingToken) {
    if (offendingToken == null) {
      return query.length() > CONTEXT_TRUNCATION_THRESHOLD
          ? "..." + query.substring(query.length() - CONTEXT_TRUNCATION_THRESHOLD)
          : query;
    }
    int contextStartIndex = offendingToken.getStartIndex() - CONTEXT_TRUNCATION_THRESHOLD;
    if (contextStartIndex < 3) {
      return query.substring(0, offendingToken.getStopIndex() + 1);
    }
    return "..." + query.substring(contextStartIndex, offendingToken.getStopIndex() + 1);
  }

  /** Human-readable summary of what the parser expected at the error position. */
  private static String getDetails(
      Recognizer<?, ?> recognizer, String msg, RecognitionException e) {
    if (e == null) return msg == null ? "" : msg;
    IntervalSet expected = e.getExpectedTokens();
    if (expected == null || expected.size() == 0) return msg == null ? "" : msg;
    List<Integer> types = expected.toList();
    if (types.isEmpty()) return msg == null ? "" : msg;
    Vocabulary vocab = recognizer.getVocabulary();
    List<String> names = new ArrayList<>(EXPECTED_TOKENS_THRESHOLD);
    for (int type : types.subList(0, Math.min(types.size(), EXPECTED_TOKENS_THRESHOLD))) {
      names.add(vocab.getDisplayName(type));
    }
    if (types.size() > EXPECTED_TOKENS_THRESHOLD) {
      return String.format(
          Locale.ROOT,
          "Expecting one of %d possible tokens. Some examples: %s, ...",
          types.size(),
          String.join(", ", names));
    }
    return "Expecting tokens: " + String.join(", ", names);
  }
}
