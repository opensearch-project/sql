/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

/** SQL syntax parser which encapsulates an ANTLR parser. */
public class SQLSyntaxParser implements Parser {
  private static final Logger LOG = LogManager.getLogger(SQLSyntaxParser.class);

  /**
   * Parse a SQL query by ANTLR parser.
   *
   * @param query a SQL query
   * @return parse tree root
   */
  @Override
  public ParseTree parse(String query) {
    AnonymizerListener anonymizer = new AnonymizerListener();

    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(createLexer(query)));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    parser.addParseListener(anonymizer);

    ParseTree parseTree = parser.root();
    LOG.info("New Engine Request Query: {}", anonymizer.getAnonymizedQueryString());

    return parseTree;
  }

  /**
   * Check if a SQL query has no statement, i.e. the parser sees nothing in it but semicolons.
   * Whitespace and comments are skipped by the lexer the same way as in {@link #parse(String)}.
   *
   * @param query a SQL query
   * @return true if the query has no statement
   */
  public static boolean hasNoStatement(String query) {
    return createLexer(query).getAllTokens().stream()
        .allMatch(
            token ->
                token.getChannel() != Token.DEFAULT_CHANNEL
                    || token.getType() == OpenSearchSQLLexer.SEMI);
  }

  private static OpenSearchSQLLexer createLexer(String query) {
    return new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
  }
}
