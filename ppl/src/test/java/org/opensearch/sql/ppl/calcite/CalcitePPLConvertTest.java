/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/** Unit tests for PPL convert command. */
public class CalcitePPLConvertTest extends CalcitePPLAbstractTest {

  public CalcitePPLConvertTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testConvertBasic() {
    String ppl = "source=EMP | convert auto(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[AUTO($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithAlias() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], salary_num=[AUTO($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `salary_num`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMultipleFunctions() {
    String ppl = "source=EMP | convert auto(SAL), num(COMM)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[AUTO($5)],"
            + " COMM=[NUM($6)], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`) `SAL`, NUM(`COMM`) `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMultipleWithAliases() {
    String ppl = "source=EMP | convert auto(SAL) AS salary, num(COMM) AS commission";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], salary=[AUTO($5)], commission=[NUM($6)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `salary`, NUM(`COMM`) `commission`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithFields() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num | fields EMPNO, ENAME, salary_num";
    RelNode root = getRelNode(ppl);
    // Calcite optimizes the two projections into one - this is more efficient
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], salary_num=[AUTO($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, AUTO(`SAL`) `salary_num`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertNumFunction() {
    String ppl = "source=EMP | convert num(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[NUM($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, NUM(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertRmcommaFunction() {
    String ppl = "source=EMP | convert rmcomma(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[RMCOMMA($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, RMCOMMA(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertRmunitFunction() {
    String ppl = "source=EMP | convert rmunit(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[RMUNIT($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, RMUNIT(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertNoneFunction() {
    String ppl = "source=EMP | convert none(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[NONE($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, NONE(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithWhere() {
    String ppl = "source=EMP | where DEPTNO = 10 | convert auto(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[AUTO($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithSort() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num | sort - salary_num | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$8], dir0=[DESC-nulls-last], fetch=[3])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], salary_num=[AUTO($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `salary_num`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 9 DESC\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithStats() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num | stats avg(salary_num) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(avg(salary_num)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], avg(salary_num)=[AVG($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], salary_num=[AUTO($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT AVG(AUTO(`SAL`)) `avg(salary_num)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertAllFunctions() {
    String ppl =
        "source=EMP | convert auto(SAL) AS sal_auto, num(COMM) AS comm_num, rmcomma(ENAME) AS"
            + " name_clean, rmunit(JOB) AS job_clean, none(EMPNO) AS empno_same";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], sal_auto=[AUTO($5)], comm_num=[NUM($6)],"
            + " name_clean=[RMCOMMA($1)], job_clean=[RMUNIT($2)], empno_same=[NONE($0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `sal_auto`, NUM(`COMM`) `comm_num`, RMCOMMA(`ENAME`) `name_clean`, RMUNIT(`JOB`)"
            + " `job_clean`, NONE(`EMPNO`) `empno_same`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
