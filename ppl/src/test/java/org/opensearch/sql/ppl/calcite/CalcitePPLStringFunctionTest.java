/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLStringFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLStringFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLower() {
    String ppl = "source=EMP | eval lower_name = lower(ENAME) | fields lower_name";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(lower_name=[LOWER($1)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "lower_name=smith\n"
            + "lower_name=allen\n"
            + "lower_name=ward\n"
            + "lower_name=jones\n"
            + "lower_name=martin\n"
            + "lower_name=blake\n"
            + "lower_name=clark\n"
            + "lower_name=scott\n"
            + "lower_name=king\n"
            + "lower_name=turner\n"
            + "lower_name=adams\n"
            + "lower_name=james\n"
            + "lower_name=ford\n"
            + "lower_name=miller\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT LOWER(`ENAME`) `lower_name`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLike() {
    String ppl = "source=EMP | where like(JOB, 'SALE%') | stats count() as cnt";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], cnt=[COUNT()])\n"
            + "  LogicalFilter(condition=[LIKE($2, 'SALE%')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "cnt=4\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT COUNT(*) `cnt`\n" + "FROM `scott`.`EMP`\n" + "WHERE `JOB` LIKE 'SALE%'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
