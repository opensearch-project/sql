/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLStreamstatsTest extends CalcitePPLAbstractTest {

  public CalcitePPLStreamstatsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testStreamstatsBy() {
    String ppl = "source=EMP | streamstats max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[MAX($5) OVER (PARTITION BY $7 ROWS UNBOUNDED"
            + " PRECEDING)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`SAL`)"
            + " OVER (PARTITION BY `DEPTNO` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            + " `max(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsCurrent() {
    String ppl = "source=EMP | streamstats current = false max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[MAX($5) OVER (PARTITION BY $7 ROWS BETWEEN"
            + " UNBOUNDED PRECEDING AND 1 PRECEDING)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`SAL`)"
            + " OVER (PARTITION BY `DEPTNO` ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
            + " `max(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsWindow() {
    String ppl = "source=EMP | streamstats window = 5 max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[MAX($5) OVER (PARTITION BY $7 ROWS 4"
            + " PRECEDING)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`SAL`)"
            + " OVER (PARTITION BY `DEPTNO` ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) `max(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
