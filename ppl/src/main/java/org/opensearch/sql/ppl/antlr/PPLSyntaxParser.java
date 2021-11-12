/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/**
 * PPL Syntax Parser.
 */
public class PPLSyntaxParser {
  /**
   * Analyze the query syntax.
   */
  public ParseTree analyzeSyntax(String query) {
    OpenSearchPPLParser parser = createParser(createLexer(query));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.root();
  }

  private OpenSearchPPLParser createParser(Lexer lexer) {
    return new OpenSearchPPLParser(
        new CommonTokenStream(lexer));
  }

  private OpenSearchPPLLexer createLexer(String query) {
    return new OpenSearchPPLLexer(
        new CaseInsensitiveCharStream(query));
  }
}
