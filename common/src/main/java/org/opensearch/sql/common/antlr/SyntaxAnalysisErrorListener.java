/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.IntervalSet;

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

    throw new SyntaxCheckException(
        String.format(
            Locale.ROOT,
            "[%s] is not a valid term at this part of the query: '%s' <-- HERE. %s",
            getOffendingText(offendingToken),
            truncateQueryAtOffendingToken(query, offendingToken),
            getDetails(recognizer, msg, e)));
  }

  private String getOffendingText(Token offendingToken) {
    return offendingToken.getText();
  }

  private String truncateQueryAtOffendingToken(String query, Token offendingToken) {
    int contextStartIndex = offendingToken.getStartIndex() - CONTEXT_TRUNCATION_THRESHOLD;
    if (contextStartIndex < 3) { // The ellipses won't save us anything below the first 4 characters
      return query.substring(0, offendingToken.getStopIndex() + 1);
    }
    return "..." + query.substring(contextStartIndex, offendingToken.getStopIndex() + 1);
  }

  private String getDetails(Recognizer<?, ?> recognizer, String msg, RecognitionException e) {
    if (e == null) {
      // According to the ANTLR docs, e == null means the parser was able to recover from the error.
      // In such cases, `msg` includes the raw error information we care about.
      return msg;
    }

    IntervalSet followSet = e.getExpectedTokens();
    Vocabulary vocab = recognizer.getVocabulary();
    List<String> tokenNames = new ArrayList<>(followSet.size());
    for (int tokenType : followSet.toList()) {
      tokenNames.add(vocab.getDisplayName(tokenType));
    }

    StringBuilder details = new StringBuilder("Expecting ");
    if (tokenNames.size() > SUGGESTION_TRUNCATION_THRESHOLD) {
      details.append("one of ").append(tokenNames.size()).append(" possible tokens. ");
      details.append("Some examples: ");
      for (int i = 0; i < SUGGESTION_TRUNCATION_THRESHOLD; i++) {
        if (i > 0) details.append(", ");
        details.append(tokenNames.get(i));
      }
      details.append(", ...");
    } else {
      details.append("tokens: ").append(String.join(", ", tokenNames));
    }
    return details.toString();
  }
}
