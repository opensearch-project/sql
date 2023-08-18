/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.legacy.domain.MethodField;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.parser.FieldMaker;

public class FieldMakerTest {

  private static final String ALIAS = "a";

  private static final String TABLE_ALIAS = "t";

  private FieldMaker fieldMaker;

  @Before
  public void init() {
    fieldMaker = new FieldMaker();
  }

  @Test
  public void makeFieldAssign() throws SqlParseException {
    final SQLIntegerExpr sqlExpr = new SQLIntegerExpr(10);
    final MethodField field = (MethodField) fieldMaker.makeField(sqlExpr, ALIAS, TABLE_ALIAS);

    assertEquals("script", field.getName());
    assertEquals(ALIAS, field.getParams().get(0).value);
    assertTrue(
        ((String) field.getParams().get(1).value)
            .matches("def assign_[0-9]+ = 10;return assign_[0-9]+;"));
    assertEquals(ALIAS, field.getAlias());
  }

  @Test
  public void makeFieldAssignDouble() throws SqlParseException {
    final SQLNumberExpr sqlExpr = new SQLNumberExpr(10.0);
    final MethodField field = (MethodField) fieldMaker.makeField(sqlExpr, ALIAS, TABLE_ALIAS);

    assertEquals("script", field.getName());
    assertEquals(ALIAS, field.getParams().get(0).value);
    assertTrue(
        ((String) field.getParams().get(1).value)
            .matches("def assign_[0-9]+ = 10.0;return assign_[0-9]+;"));
    assertEquals(ALIAS, field.getAlias());
  }
}
