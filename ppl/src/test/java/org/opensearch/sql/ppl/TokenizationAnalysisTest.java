/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.FileWriter;
import java.io.IOException;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.junit.Test;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;

public class TokenizationAnalysisTest {

  @Test
  public void analyzeTokenization() throws IOException {
    String[] inputs = {"c:t", "c:.t", ".t", "t", "c:test", "c:.test"};

    try (FileWriter writer = new FileWriter("/tmp/tokenization_output.txt")) {
      for (String input : inputs) {
        writer.write("\n=== Tokenizing: '" + input + "' ===\n");
        OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        for (Token token : tokens.getTokens()) {
          if (token.getType() != Token.EOF) {
            String tokenName = OpenSearchPPLLexer.VOCABULARY.getSymbolicName(token.getType());
            writer.write(
                "  Token["
                    + token.getType()
                    + "]: "
                    + tokenName
                    + " = '"
                    + token.getText()
                    + "'\n");
          }
        }
      }
    }
  }
}
