/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;
import org.opensearch.sql.legacy.parser.ElasticSqlExprParser;
import org.opensearch.sql.legacy.rewriter.parent.SQLExprParentSetter;

/** Test utils class include all SQLExpr related method. */
public class SqlParserUtils {

  /**
   * Parse sql with {@link ElasticSqlExprParser}
   *
   * @param sql sql
   * @return {@link SQLQueryExpr}
   */
  public static SQLQueryExpr parse(String sql) {
    ElasticSqlExprParser parser = new ElasticSqlExprParser(sql);
    SQLExpr expr = parser.expr();
    if (parser.getLexer().token() != Token.EOF) {
      throw new ParserException("Illegal sql: " + sql);
    }
    SQLQueryExpr queryExpr = (SQLQueryExpr) expr;
    queryExpr.accept(new SQLExprParentSetter());
    return (SQLQueryExpr) expr;
  }
}
