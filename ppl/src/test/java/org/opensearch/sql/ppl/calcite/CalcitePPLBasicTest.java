/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Ignore;
import org.junit.Test;

public class CalcitePPLBasicTest extends CalcitePPLAbstractTest {

  public CalcitePPLBasicTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testInvalidTable() {
    String ppl = "source=unknown";
    try {
      RelNode root = getRelNode(ppl);
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'unknown' not found"));
    }
  }

  @Test
  public void testScanTable() {
    String ppl = "source=products_temporal";
    RelNode root = getRelNode(ppl);
    verifyLogical(root, "LogicalTableScan(table=[[scott, products_temporal]])\n");
  }

  @Test
  public void testScanTableTwoParts() {
    String ppl = "source=`scott`.`products_temporal`";
    RelNode root = getRelNode(ppl);
    verifyLogical(root, "LogicalTableScan(table=[[scott, products_temporal]])\n");
  }

  @Test
  public void testFilterQuery() {
    String ppl = "source=scott.products_temporal | where SUPPLIER > 0 AND ID = '1000'";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[AND(>($1, 0), =($0, '1000':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`products_temporal`\n"
            + "WHERE `SUPPLIER` > 0 AND `ID` = '1000'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterQueryWithBetween() {
    String ppl = "source=EMP | where DEPTNO between 20 and 30 | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[SEARCH($7, Sarg[[20..30]])])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` >= 20 AND `DEPTNO` <= 30";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterQueryWithBetween2() {
    String ppl = "source=EMP | where DEPTNO between 20 and 30.0 | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[SEARCH($7, Sarg[[20.0:DECIMAL(11, 1)..30.0:DECIMAL(11,"
            + " 1)]]:DECIMAL(11, 1))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` >= 20.0 AND `DEPTNO` <= 30.0";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterQueryWithOr() {
    String ppl =
        "source=EMP | where (DEPTNO = 20 or MGR = 30) and SAL > 1000 | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[AND(OR(=($7, 20), =($3, 30)), >($5, 1000))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO` = 20 OR `MGR` = 30) AND `SAL` > 1000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterQueryWithOr2() {
    String ppl = "source=EMP (DEPTNO = 20 or MGR = 30) and SAL > 1000 | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[AND(OR(=($7, 20), =($3, 30)), >($5, 1000))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO` = 20 OR `MGR` = 30) AND `SAL` > 1000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterQueryWithIn() {
    String ppl = "source=scott.products_temporal | where ID in ('1000', '2000')";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[SEARCH($0, Sarg['1000':VARCHAR,"
            + " '2000':VARCHAR]:VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\nFROM `scott`.`products_temporal`\nWHERE `ID` IN ('1000', '2000')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterQueryWithIn2() {
    String ppl = "source=EMP |  where DEPTNO in (20, 30.0)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[SEARCH($7, Sarg[20.0:DECIMAL(11, 1), 30.0:DECIMAL(11,"
            + " 1)]:DECIMAL(11, 1))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\nFROM `scott`.`EMP`\nWHERE `DEPTNO` IN (20.0, 30.0)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testQueryWithFields() {
    String ppl = "source=products_temporal | fields SUPPLIER, ID";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(SUPPLIER=[$1], ID=[$0])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "" + "SELECT `SUPPLIER`, `ID`\n" + "FROM `scott`.`products_temporal`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testQueryMinusFields() {
    String ppl = "source=products_temporal | fields - SUPPLIER, ID";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(SYS_START=[$2], SYS_END=[$3])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "" + "SELECT `SYS_START`, `SYS_END`\n" + "FROM `scott`.`products_temporal`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsPlusThenMinus() {
    String ppl = "source=EMP | fields + EMPNO, DEPTNO, SAL | fields - DEPTNO, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "" + "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testFieldsMinusThenPlusShouldThrowException() {
    String ppl = "source=EMP | fields - DEPTNO, SAL | fields + EMPNO, DEPTNO, SAL";
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              RelNode root = getRelNode(ppl);
            });
    assertThat(
        e.getMessage(),
        is("field [DEPTNO] not found; input fields are: [EMPNO, ENAME, JOB, MGR, HIREDATE, COMM]"));
  }

  @Test
  public void testScanTableAndCheckResults() {
    String ppl = "source=EMP | where DEPTNO = 20";
    RelNode root = getRelNode(ppl);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT *\n" + "FROM `scott`.`EMP`\n" + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSort() {
    String ppl = "source=EMP | sort DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "" + "LogicalSort(sort0=[$7], dir0=[ASC])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testSortTwoFields() {
    String ppl = "source=EMP | sort DEPTNO, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$7], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testSortWithDesc() {
    String ppl = "source=EMP | sort + DEPTNO, - SAL | fields EMPNO, DEPTNO, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], DEPTNO=[$7], SAL=[$5])\n"
            + "  LogicalSort(sort0=[$7], sort1=[$5], dir0=[ASC], dir1=[DESC])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "EMPNO=7839; DEPTNO=10; SAL=5000.00\n"
            + "EMPNO=7782; DEPTNO=10; SAL=2450.00\n"
            + "EMPNO=7934; DEPTNO=10; SAL=1300.00\n"
            + "EMPNO=7788; DEPTNO=20; SAL=3000.00\n"
            + "EMPNO=7902; DEPTNO=20; SAL=3000.00\n"
            + "EMPNO=7566; DEPTNO=20; SAL=2975.00\n"
            + "EMPNO=7876; DEPTNO=20; SAL=1100.00\n"
            + "EMPNO=7369; DEPTNO=20; SAL=800.00\n"
            + "EMPNO=7698; DEPTNO=30; SAL=2850.00\n"
            + "EMPNO=7499; DEPTNO=30; SAL=1600.00\n"
            + "EMPNO=7844; DEPTNO=30; SAL=1500.00\n"
            + "EMPNO=7521; DEPTNO=30; SAL=1250.00\n"
            + "EMPNO=7654; DEPTNO=30; SAL=1250.00\n"
            + "EMPNO=7900; DEPTNO=30; SAL=950.00\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testSortWithDescAndLimit() {
    String ppl = "source=EMP | sort - SAL | fields EMPNO, DEPTNO, SAL | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], DEPTNO=[$7], SAL=[$5])\n"
            + "  LogicalSort(sort0=[$5], dir0=[DESC], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "EMPNO=7839; DEPTNO=10; SAL=5000.00\n"
            + "EMPNO=7788; DEPTNO=20; SAL=3000.00\n"
            + "EMPNO=7902; DEPTNO=20; SAL=3000.00\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testMultipleTables() {
    String ppl = "source=EMP, EMP";
    try {
      RelNode root = getRelNode(ppl);
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'EMP,EMP' not found"));
    }
  }

  @Test
  public void testMultipleTablesAndFilters() {
    String ppl = "source=EMP, EMP DEPTNO = 20 | fields EMPNO, DEPTNO, SAL";
    try {
      RelNode root = getRelNode(ppl);
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'EMP,EMP' not found"));
    }
  }

  @Ignore
  public void testLineComments() {
    String ppl1 = "source=products_temporal  //this is a comment";
    verifyLogical(getRelNode(ppl1), "LogicalTableScan(table=[[scott, products_temporal]])\n");
    String ppl2 = "source=products_temporal  // this is a comment";
    verifyLogical(getRelNode(ppl2), "LogicalTableScan(table=[[scott, products_temporal]])\n");
    String ppl3 =
        ""
            + "// test is a new line comment\n"
            + "source=products_temporal  // this is a comment\n"
            + "| fields SUPPLIER, ID  // this is line comment inner ppl command\n"
            + "////this is a new line comment";
    String expectedLogical =
        ""
            + "LogicalProject(SUPPLIER=[$1], ID=[$0])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(getRelNode(ppl3), expectedLogical);
  }

  @Ignore
  public void testBlockComments() {
    String ppl1 = "/* this is a block comment */ source=products_temporal";
    verifyLogical(getRelNode(ppl1), "LogicalTableScan(table=[[scott, products_temporal]])\n");
    String ppl2 = "source=products_temporal | /*this is a block comment*/ fields SUPPLIER, ID";
    String expectedLogical2 =
        ""
            + "LogicalProject(SUPPLIER=[$1], ID=[$0])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(getRelNode(ppl2), expectedLogical2);
    String ppl3 =
        ""
            + "/*\n"
            + " * This is a\n"
            + " *   multiple\n"
            + " * line\n"
            + " *   block\n"
            + " *     comment\n"
            + " */\n"
            + "search /* block comment */ source=products_temporal /* block comment */ ID = 0\n"
            + "| /*\n"
            + "     This is a\n"
            + "       multiple\n"
            + "     line\n"
            + "       block\n"
            + "         comment */ fields SUPPLIER, ID /* block comment */\n"
            + "/* block comment */";
    String expectedLogical3 =
        ""
            + "LogicalProject(SUPPLIER=[$1], ID=[$0])\n"
            + "  LogicalFilter(condition=[=($0, 0)])\n"
            + "    LogicalTableScan(table=[[scott, products_temporal]])\n";
    verifyLogical(getRelNode(ppl3), expectedLogical3);
  }

  @Test
  public void testTableAlias() {
    String ppl =
        "source=EMP as e | where (e.DEPTNO = 20 or e.MGR = 30) and e.SAL > 1000 | fields e.EMPNO,"
            + " e.ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[AND(OR(=($7, 20), =($3, 30)), >($5, 1000))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO` = 20 OR `MGR` = 30) AND `SAL` > 1000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRelationSubqueryAlias() {
    String ppl = "source=EMP as e | join on e.DEPTNO = d.DEPTNO [ source=DEPT | head 10 ] as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalSort(fetch=[10])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `t`.`DEPTNO` `d.DEPTNO`, `t`.`DNAME`,"
            + " `t`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN (SELECT `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM `scott`.`DEPT`\n"
            + "LIMIT 10) `t` ON `EMP`.`DEPTNO` = `t`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRename() {
    String ppl = "source=EMP | rename DEPTNO as DEPTNO_E";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO_E=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO` `DEPTNO_E`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDecimalLiteral() {
    String ppl =
        "source=EMP | eval r1 = 22 / 7.0, r2 = 22 / 7.0d, r3 = 22.0 / 7, r4 = 22.0d / 7,"
            + " r5 = 0.1 * 0.2, r6 = 0.1d * 0.2d, r7 = 0.1 + 0.2, r8 = 0.1d + 0.2d,"
            + " r9 = 0.06 - 0.01, r10 = 0.06d - 0.01d, r11 = 0.1 / 0.3 * 0.3,"
            + " r12 = 0.1d / 0.3d * 0.3d, r13 = pow(sqrt(2.0), 2), r14 = pow(sqrt(2.0d), 2),"
            + " r15 = 7.0 / 0, r16 = 7 / 0.0"
            + " | fields r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16"
            + " | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(r1=[DIVIDE(22, 7.0:DECIMAL(2, 1))], r2=[DIVIDE(22, 7.0E0:DOUBLE)],"
            + " r3=[DIVIDE(22.0:DECIMAL(3, 1), 7)], r4=[DIVIDE(22.0E0:DOUBLE, 7)],"
            + " r5=[*(0.1:DECIMAL(2, 1), 0.2:DECIMAL(2, 1))], r6=[*(0.1E0:DOUBLE, 0.2E0:DOUBLE)],"
            + " r7=[+(0.1:DECIMAL(2, 1), 0.2:DECIMAL(2, 1))], r8=[+(0.1E0:DOUBLE, 0.2E0:DOUBLE)],"
            + " r9=[-(0.06:DECIMAL(3, 2), 0.01:DECIMAL(3, 2))], r10=[-(0.06E0:DOUBLE,"
            + " 0.01E0:DOUBLE)], r11=[*(DIVIDE(0.1:DECIMAL(2, 1), 0.3:DECIMAL(2, 1)),"
            + " 0.3:DECIMAL(2, 1))], r12=[*(DIVIDE(0.1E0:DOUBLE, 0.3E0:DOUBLE), 0.3E0)],"
            + " r13=[POWER(POWER(2.0:DECIMAL(2, 1), 0.5E0:DOUBLE), 2)],"
            + " r14=[POWER(POWER(2.0E0:DOUBLE, 0.5E0:DOUBLE), 2)],"
            + " r15=[DIVIDE(7.0:DECIMAL(2, 1), 0)], r16=[DIVIDE(7, 0.0:DECIMAL(2, 1))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DIVIDE`(22, 7.0) `r1`, `DIVIDE`(22, 7.0E0) `r2`, `DIVIDE`(22.0, 7) `r3`,"
            + " `DIVIDE`(2.20E1, 7) `r4`, 0.1 * 0.2 `r5`, 1E-1 * 2E-1 `r6`, 0.1 + 0.2 `r7`, 1E-1 +"
            + " 2E-1 `r8`, 0.06 - 0.01 `r9`, 6E-2 - 1E-2 `r10`, `DIVIDE`(0.1, 0.3) * 0.3 `r11`,"
            + " `DIVIDE`(1E-1, 3E-1) * 3E-1 `r12`, POWER(POWER(2.0, 5E-1), 2) `r13`,"
            + " POWER(POWER(2.0E0, 5E-1), 2) `r14`, `DIVIDE`(7.0, 0) `r15`, `DIVIDE`(7, 0.0)"
            + " `r16`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
