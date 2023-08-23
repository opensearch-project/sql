/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.subquery;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.opensearch.sql.legacy.rewriter.subquery.SubQueryRewriteRule;
import org.opensearch.sql.legacy.util.SqlParserUtils;

public abstract class SubQueryRewriterTestBase {

  SQLQueryExpr expr(String query) {
    return SqlParserUtils.parse(query);
  }

  SQLQueryExpr rewrite(SQLQueryExpr expr) {
    new SubQueryRewriteRule().rewrite(expr);
    return expr;
  }

  String sqlString(SQLObject expr) {
    return SQLUtils.toMySqlString(expr)
        .replaceAll("\n", " ")
        .replaceAll("\t", " ")
        .replaceAll(" +", " ");
  }
}
