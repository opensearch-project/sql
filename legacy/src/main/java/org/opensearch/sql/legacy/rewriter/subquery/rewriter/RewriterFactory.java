/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.subquery.rewriter;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.opensearch.sql.legacy.rewriter.subquery.RewriterContext;

/** Factory for generating the {@link Rewriter}. */
public class RewriterFactory {

  /** Create list of {@link Rewriter}. */
  public static List<Rewriter> createRewriterList(SQLExpr expr, RewriterContext bb) {
    if (expr instanceof SQLExistsExpr) {
      return existRewriterList((SQLExistsExpr) expr, bb);
    } else if (expr instanceof SQLInSubQueryExpr) {
      return inRewriterList((SQLInSubQueryExpr) expr, bb);
    }
    return ImmutableList.of();
  }

  private static List<Rewriter> existRewriterList(SQLExistsExpr existsExpr, RewriterContext bb) {
    return new ImmutableList.Builder<Rewriter>()
        .add(new NestedExistsRewriter(existsExpr, bb))
        .build();
  }

  private static List<Rewriter> inRewriterList(SQLInSubQueryExpr inExpr, RewriterContext bb) {
    return new ImmutableList.Builder<Rewriter>().add(new InRewriter(inExpr, bb)).build();
  }
}
