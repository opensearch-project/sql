/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLBinTest extends CalcitePPLAbstractTest {

  public CalcitePPLBinTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testBinWithSpan() {
    String ppl = "source=EMP | bin SAL span=1000";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
            + "COMM=[$6], DEPTNO=[$7], SAL_bin=[*(FLOOR(/($5, 1000)), 1000)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "FLOOR(`SAL` / 1000) * 1000 `SAL_bin`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinWithSpanAndAlias() {
    String ppl = "source=EMP | bin SAL span=500 AS salary_range";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
            + "COMM=[$6], DEPTNO=[$7], salary_range=[*(FLOOR(/($5, 500)), 500)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "FLOOR(`SAL` / 500) * 500 `salary_range`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinWithBinsParameter() {
    String ppl = "source=EMP | bin SAL bins=5";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
            + "COMM=[$6], DEPTNO=[$7], SAL_bin=[*(FLOOR(/(-($5, 800.0E0:DOUBLE), /(4200.0E0:DOUBLE, 5))), /(4200.0E0:DOUBLE, 5))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "FLOOR((`SAL` - 8.000E2) / (4.2000E3 / 5)) * (4.2000E3 / 5) `SAL_bin`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinWithIntegrationWithStats() {
    String ppl = "source=EMP | bin SAL span=1000 AS salary_bin | stats count() by salary_bin";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count()=[$1], salary_bin=[$0])\n"
            + "  LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "    LogicalProject(salary_bin=[*(FLOOR(/($5, 1000)), 1000)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count()`, FLOOR(`SAL` / 1000) * 1000 `salary_bin`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY FLOOR(`SAL` / 1000) * 1000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinWithStatsAndAverage() {
    String ppl = "source=EMP | bin SAL span=1000 AS salary_bin | stats avg(SAL) as avg_salary by salary_bin";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(avg_salary=[$1], salary_bin=[$0])\n"
            + "  LogicalAggregate(group=[{0}], avg_salary=[AVG($1)])\n"
            + "    LogicalProject(salary_bin=[*(FLOOR(/($5, 1000)), 1000)], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg_salary`, FLOOR(`SAL` / 1000) * 1000 `salary_bin`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY FLOOR(`SAL` / 1000) * 1000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinMathematicalCorrectness() {
    String ppl = "source=EMP | bin SAL span=1000 | fields ENAME, SAL, SAL_bin | head 5";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[5])\n"
            + "  LogicalProject(ENAME=[$1], SAL=[$5], SAL_bin=[*(FLOOR(/($5, 1000)), 1000)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // Verify that the binning calculation produces correct mathematical results
    String expectedResult =
        "ENAME=SMITH; SAL=800.00; SAL_bin=0\n"
            + "ENAME=ALLEN; SAL=1600.00; SAL_bin=1000\n"
            + "ENAME=WARD; SAL=1250.00; SAL_bin=1000\n"
            + "ENAME=JONES; SAL=2975.00; SAL_bin=2000\n"
            + "ENAME=MARTIN; SAL=1250.00; SAL_bin=1000\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, `SAL`, FLOOR(`SAL` / 1000) * 1000 `SAL_bin`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinWithDecimalSpan() {
    String ppl = "source=EMP | bin SAL span=750.5 AS salary_group";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
            + "COMM=[$6], DEPTNO=[$7], salary_group=[*(FLOOR(/($5, 750.5:DECIMAL(4, 1))), "
            + "750.5:DECIMAL(4, 1))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "FLOOR(`SAL` / 750.5) * 750.5 `salary_group`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinDefaultBehavior() {
    String ppl = "source=EMP | bin SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
            + "COMM=[$6], DEPTNO=[$7], SAL_bin=[FLOOR(/($5, 1))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "FLOOR(`SAL` / 1) `SAL_bin`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}