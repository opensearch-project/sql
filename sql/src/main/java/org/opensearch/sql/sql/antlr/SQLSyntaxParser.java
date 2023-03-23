/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

/**
 * SQL syntax parser which encapsulates an ANTLR parser.
 */
public class SQLSyntaxParser implements Parser {

  /**
   * Parse a SQL query by ANTLR parser.
   * @param query   a SQL query
   * @return        parse tree root
   */
  @Override
  public ParseTree parse(String query) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.root();
  }

  /**
   * Checks if the query contains hints as it is not yet support in V2.
   * @param query   a SQL query
   * @return        boolean value if query contains hints
   */
  public ParseTree parseHints(String query) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchSQLParser hintsParser = new OpenSearchSQLParser(
        new CommonTokenStream(lexer, OpenSearchSQLLexer.SQLCOMMENT));
    return hintsParser.root();
  }
}
