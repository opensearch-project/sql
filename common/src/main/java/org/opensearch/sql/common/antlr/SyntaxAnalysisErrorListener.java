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
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;

/**
 * Syntax analysis error listener that handles any syntax error by throwing exception with useful
 * information.
 */
public class SyntaxAnalysisErrorListener extends BaseErrorListener {
  // Show up to this many characters before the offending token in the query.
  private static final int CONTEXT_TRUNCATION_THRESHOLD = 20;
  // Avoid presenting too many alternatives when many are available.
  private static final int SUGGESTION_TRUNCATION_THRESHOLD = 5;

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

    String offendingText = getOffendingText(offendingToken);
    String errorContext = truncateQueryAtOffendingToken(query, offendingToken);
    String details = getDetails(recognizer, msg, e);
    int expectedTokenCount = getExpectedTokenCount(recognizer, e);

    String errorMessage =
        String.format(
            Locale.ROOT,
            "[%s] is not a valid term at this part of the query: '%s' <-- HERE. %s",
            offendingText,
            errorContext,
            details);

    SyntaxCheckException syntaxException = new SyntaxCheckException(errorMessage);

    ErrorReport.Builder builder =
        ErrorReport.wrap(syntaxException)
            .code(ErrorCode.SYNTAX_ERROR)
            .stage(QueryProcessingStage.ANALYZING)
            .location("while parsing query syntax")
            .context("offending_token", offendingText)
            .context("line", line)
            .context("char_position", charPositionInLine)
            .context("error_context", errorContext)
            .context("expected_token_count", expectedTokenCount);

    // Add token position info only if offendingToken is not null
    if (offendingToken != null) {
      builder
          .context("token_start", offendingToken.getStartIndex())
          .context("token_end", offendingToken.getStopIndex());
    }

    throw builder.build();
  }

  private String getOffendingText(Token offendingToken) {
    return offendingToken != null ? offendingToken.getText() : "<unknown>";
  }

  private String truncateQueryAtOffendingToken(String query, Token offendingToken) {
    if (offendingToken == null) {
      return query.length() > CONTEXT_TRUNCATION_THRESHOLD
          ? "..." + query.substring(query.length() - CONTEXT_TRUNCATION_THRESHOLD)
          : query;
    }

    int contextStartIndex = offendingToken.getStartIndex() - CONTEXT_TRUNCATION_THRESHOLD;
    if (contextStartIndex < 3) { // The ellipses won't save us anything below the first 4 characters
      return query.substring(0, offendingToken.getStopIndex() + 1);
    }
    return "..." + query.substring(contextStartIndex, offendingToken.getStopIndex() + 1);
  }

  private List<String> topSuggestions(Recognizer<?, ?> recognizer, IntervalSet continuations) {
    Vocabulary vocab = recognizer.getVocabulary();
    List<String> tokenNames = new ArrayList<>(SUGGESTION_TRUNCATION_THRESHOLD);
    for (int tokenType :
        continuations
            .toList()
            .subList(0, Math.min(continuations.size(), SUGGESTION_TRUNCATION_THRESHOLD))) {
      tokenNames.add(vocab.getDisplayName(tokenType));
    }
    return tokenNames;
  }

  private String getDetails(Recognizer<?, ?> recognizer, String msg, RecognitionException ex) {
    if (ex == null) {
      // According to the ANTLR docs, ex == null means the parser was able to recover from the
      // error.
      // In such cases, `msg` includes the raw error information we care about.
      return msg;
    }

    IntervalSet possibleContinuations = ex.getExpectedTokens();
    List<String> suggestions = topSuggestions(recognizer, possibleContinuations);

    StringBuilder details = new StringBuilder("Expecting ");
    if (possibleContinuations.size() > SUGGESTION_TRUNCATION_THRESHOLD) {
      details
          .append("one of ")
          .append(possibleContinuations.size())
          .append(" possible tokens. Some examples: ")
          .append(String.join(", ", suggestions))
          .append(", ...");
    } else {
      details.append("tokens: ").append(String.join(", ", suggestions));
    }
    return details.toString();
  }

  private int getExpectedTokenCount(Recognizer<?, ?> recognizer, RecognitionException ex) {
    if (ex == null) {
      return 0;
    }
    IntervalSet expectedTokens = ex.getExpectedTokens();
    return expectedTokens != null ? expectedTokens.size() : 0;
  }
}
