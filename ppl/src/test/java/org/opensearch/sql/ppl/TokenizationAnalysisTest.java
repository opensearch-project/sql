/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertTrue;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.junit.Test;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;

public class TokenizationAnalysisTest {

  @Test
  public void analyzeTokenization() {
    String[] inputs = {"c:t", "c:.t", ".t", "t", "c:test", "c:.test"};

    for (String input : inputs) {
      OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(input));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      tokens.fill();

      // Verify tokenization succeeds and produces tokens
      assertTrue("Should produce at least one token", tokens.getTokens().size() > 0);

      for (Token token : tokens.getTokens()) {
        if (token.getType() != Token.EOF) {
          String tokenName = OpenSearchPPLLexer.VOCABULARY.getSymbolicName(token.getType());
          // Verify token has valid type and text
          assertTrue("Token should have valid type", token.getType() > 0);
          assertTrue("Token should have non-null text", token.getText() != null);
        }
      }
    }
  }
}
