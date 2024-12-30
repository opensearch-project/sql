/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.subquery;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class InSubqueryRewriterTest extends SubQueryRewriterTestBase {

  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void nonCorrleatedIn() throws Exception {
    assertEquals(
        sqlString(
            expr(
                "SELECT TbA_0.* "
                    + "FROM TbA as TbA_0 "
                    + "JOIN TbB as TbB_1 "
                    + "ON TbA_0.a = TbB_1.b "
                    + "WHERE TbB_1.b IS NOT NULL")),
        sqlString(rewrite(expr("SELECT * FROM TbA " + "WHERE a in (SELECT b FROM TbB)"))));
  }

  @Test
  public void nonCorrleatedInWithWhere() throws Exception {
    assertEquals(
        sqlString(
            expr(
                "SELECT TbA_0.* "
                    + "FROM TbA as TbA_0 "
                    + "JOIN TbB as TbB_1 "
                    + "ON TbA_0.a = TbB_1.b "
                    + "WHERE TbB_1.b IS NOT NULL AND TbB_1.b > 0")),
        sqlString(
            rewrite(
                expr("SELECT * " + "FROM TbA " + "WHERE a in (SELECT b FROM TbB WHERE b > 0)"))));
  }

  @Test
  public void nonCorrleatedInWithOuterWhere() throws Exception {
    assertEquals(
        sqlString(
            expr(
                "SELECT TbA_0.* "
                    + "FROM TbA as TbA_0 "
                    + "JOIN TbB as TbB_1 "
                    + "ON TbA_0.a = TbB_1.b "
                    + "WHERE TbB_1.b IS NOT NULL AND TbA_0.a > 10")),
        sqlString(
            rewrite(
                expr("SELECT * " + "FROM TbA " + "WHERE a in (SELECT b FROM TbB) AND a > 10"))));
  }

  @Test
  public void notInUnsupported() throws Exception {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("Unsupported subquery");
    rewrite(expr("SELECT * FROM TbA WHERE a not in (SELECT b FROM TbB)"));
  }

  @Test
  public void testMultipleSelectException() throws Exception {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("Unsupported subquery with multiple select [TbB_1.b1, TbB_1.b2]");
    rewrite(expr("SELECT * " + "FROM TbA WHERE a in (SELECT b1, b2 FROM TbB) AND a > 10"));
  }
}
