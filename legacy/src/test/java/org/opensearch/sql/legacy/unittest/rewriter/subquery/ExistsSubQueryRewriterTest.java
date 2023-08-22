/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.subquery;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExistsSubQueryRewriterTest extends SubQueryRewriterTestBase {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void nonCorrelatedExists() {
    assertEquals(
        sqlString(
            expr("SELECT e.name " + "FROM employee e, e.projects p " + "WHERE p IS NOT MISSING")),
        sqlString(
            rewrite(
                expr(
                    "SELECT e.name "
                        + "FROM employee as e "
                        + "WHERE EXISTS (SELECT * FROM e.projects as p)"))));
  }

  @Test
  public void nonCorrelatedExistsWhere() {
    assertEquals(
        sqlString(
            expr(
                "SELECT e.name "
                    + "FROM employee e, e.projects p "
                    + "WHERE p IS NOT MISSING AND p.name LIKE 'security'")),
        sqlString(
            rewrite(
                expr(
                    "SELECT e.name FROM employee as e WHERE EXISTS (SELECT * FROM e.projects as p"
                        + " WHERE p.name LIKE 'security')"))));
  }

  @Test
  public void nonCorrelatedExistsParentWhere() {
    assertEquals(
        sqlString(
            expr(
                "SELECT e.name "
                    + "FROM employee e, e.projects p "
                    + "WHERE p IS NOT MISSING AND e.name LIKE 'security'")),
        sqlString(
            rewrite(
                expr(
                    "SELECT e.name FROM employee as e WHERE EXISTS (SELECT * FROM e.projects as p)"
                        + " AND e.name LIKE 'security'"))));
  }

  @Test
  public void nonCorrelatedNotExists() {
    assertEquals(
        sqlString(
            expr(
                "SELECT e.name "
                    + "FROM employee e, e.projects p "
                    + "WHERE NOT (p IS NOT MISSING)")),
        sqlString(
            rewrite(
                expr(
                    "SELECT e.name "
                        + "FROM employee as e "
                        + "WHERE NOT EXISTS (SELECT * FROM e.projects as p)"))));
  }

  @Test
  public void nonCorrelatedNotExistsWhere() {
    assertEquals(
        sqlString(
            expr(
                "SELECT e.name "
                    + "FROM employee e, e.projects p "
                    + "WHERE NOT (p IS NOT MISSING AND p.name LIKE 'security')")),
        sqlString(
            rewrite(
                expr(
                    "SELECT e.name FROM employee as e WHERE NOT EXISTS (SELECT * FROM e.projects as"
                        + " p WHERE p.name LIKE 'security')"))));
  }

  @Test
  public void nonCorrelatedNotExistsParentWhere() {
    assertEquals(
        sqlString(
            expr(
                "SELECT e.name "
                    + "FROM employee e, e.projects p "
                    + "WHERE NOT (p IS NOT MISSING) AND e.name LIKE 'security'")),
        sqlString(
            rewrite(
                expr(
                    "SELECT e.name FROM employee as e WHERE NOT EXISTS (SELECT * FROM e.projects as"
                        + " p) AND e.name LIKE 'security'"))));
  }

  @Test
  public void nonCorrelatedExistsAnd() {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("Unsupported subquery");
    rewrite(
        expr(
            "SELECT e.name FROM employee as e WHERE EXISTS (SELECT * FROM e.projects as p) AND"
                + " EXISTS (SELECT * FROM e.comments as c)"));
  }
}
