/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLTopAndRareTest extends CalcitePPLAbstractTest {

  public CalcitePPLTopAndRareTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testRare() {
    String ppl = "source=EMP | rare JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(JOB=[$0])\n"
            + "  LogicalSort(sort0=[$1], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{2}], count_JOB=[COUNT($2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `JOB`\n"
            + "FROM (SELECT `JOB`, COUNT(`JOB`) `count_JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`\n"
            + "ORDER BY 2 NULLS LAST) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareBy() {
    String ppl = "source=EMP | rare JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(JOB=[$0], DEPTNO=[$1])\n"
            + "  LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{2, 7}], count_JOB=[COUNT($2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `JOB`, `DEPTNO`\n"
            + "FROM (SELECT `JOB`, `DEPTNO`, COUNT(`JOB`) `count_JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`, `DEPTNO`\n"
            + "ORDER BY 3 NULLS LAST) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTop() {
    String ppl = "source=EMP | top JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(JOB=[$0])\n"
            + "  LogicalSort(sort0=[$1], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{2}], count_JOB=[COUNT($2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `JOB`\n"
            + "FROM (SELECT `JOB`, COUNT(`JOB`) `count_JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`\n"
            + "ORDER BY 2 NULLS LAST) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopBy() {
    String ppl = "source=EMP | top JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(JOB=[$0], DEPTNO=[$1])\n"
            + "  LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{2, 7}], count_JOB=[COUNT($2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `JOB`, `DEPTNO`\n"
            + "FROM (SELECT `JOB`, `DEPTNO`, COUNT(`JOB`) `count_JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`, `DEPTNO`\n"
            + "ORDER BY 3 NULLS LAST) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
