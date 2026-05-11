/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.suggestion.SyntaxErrorContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

/** Runs the SQL parser on a query and returns the SyntaxErrorContext from the first error. */
final class ContextFactory {
  private ContextFactory() {}

  static SyntaxErrorContext contextFor(String query) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OpenSearchSQLParser parser = new OpenSearchSQLParser(tokens);
    parser.removeErrorListeners();
    Capturing capturing = new Capturing();
    parser.addErrorListener(capturing);
    try {
      parser.root();
    } catch (RuntimeException ignored) {
      // some errors bail out of the parse
    }
    if (capturing.context == null) {
      throw new AssertionError("expected a syntax error for query: " + query);
    }
    return capturing.context;
  }

  private static class Capturing extends BaseErrorListener {
    SyntaxErrorContext context;

    @Override
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int charPositionInLine,
        String msg,
        RecognitionException e) {
      if (context != null) return;
      context =
          new SyntaxErrorContext(
              recognizer,
              (Token) offendingSymbol,
              (CommonTokenStream) recognizer.getInputStream(),
              ((CommonTokenStream) recognizer.getInputStream()).getText(),
              e);
    }
  }
}
