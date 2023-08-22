/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import java.sql.SQLFeatureNotSupportedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.sql.legacy.rewriter.RewriteRule;
import org.opensearch.sql.legacy.rewriter.RewriteRuleExecutor;

@RunWith(MockitoJUnitRunner.class)
public class RewriteRuleExecutorTest {
  @Mock private RewriteRule<SQLQueryExpr> rewriter;
  @Mock private SQLQueryExpr expr;

  private RewriteRuleExecutor<SQLQueryExpr> ruleExecutor;

  @Before
  public void setup() {
    ruleExecutor = RewriteRuleExecutor.<SQLQueryExpr>builder().withRule(rewriter).build();
  }

  @Test
  public void optimize() throws SQLFeatureNotSupportedException {
    when(rewriter.match(expr)).thenReturn(true);

    ruleExecutor.executeOn(expr);
    verify(rewriter, times(1)).rewrite(expr);
  }

  @Test
  public void noOptimize() throws SQLFeatureNotSupportedException {
    when(rewriter.match(expr)).thenReturn(false);

    ruleExecutor.executeOn(expr);
    verify(rewriter, never()).rewrite(expr);
  }
}
