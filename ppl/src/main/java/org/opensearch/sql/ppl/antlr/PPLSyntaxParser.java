/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/** PPL Syntax Parser. */
public class PPLSyntaxParser implements Parser {
  private final Settings settings;

  // visible for testing
  public PPLSyntaxParser() {
    this.settings = null;
  }

  public PPLSyntaxParser(Settings settings) {
    this.settings = settings;
  }

  /** Analyze the query syntax. */
  @Override
  public ParseTree parse(String query) {
    OpenSearchPPLParser parser = createParser(createLexer(query));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    if (settings != null) {
      parser.SPL_compatible_grammar_enabled =
          settings.getSettingValue(Settings.Key.SPL_COMPATIBLE_GRAMMAR_ENABLED);
    }
    return parser.root();
  }

  private OpenSearchPPLParser createParser(Lexer lexer) {
    return new OpenSearchPPLParser(new CommonTokenStream(lexer));
  }

  private OpenSearchPPLLexer createLexer(String query) {
    return new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
  }
}
