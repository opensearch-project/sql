/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.parent;

import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.sql.legacy.rewriter.parent.SQLExprParentSetterRule;

@RunWith(MockitoJUnitRunner.class)
public class SQLExprParentSetterRuleTest {

  @Mock private SQLQueryExpr queryExpr;

  private SQLExprParentSetterRule rule = new SQLExprParentSetterRule();

  @Test
  public void match() {
    assertTrue(rule.match(queryExpr));
  }
}
