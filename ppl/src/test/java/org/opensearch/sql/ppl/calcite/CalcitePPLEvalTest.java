/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLEvalTest extends CalcitePPLAbstractTest {

  public CalcitePPLEvalTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testEval1() {
    String ppl = "source=EMP | eval a = 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1 `a`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalAndFields() {
    String ppl = "source=EMP | eval a = 1 | fields EMPNO, a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "" + "LogicalProject(EMPNO=[$0], a=[1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "EMPNO=7369; a=1\n"
            + "EMPNO=7499; a=1\n"
            + "EMPNO=7521; a=1\n"
            + "EMPNO=7566; a=1\n"
            + "EMPNO=7654; a=1\n"
            + "EMPNO=7698; a=1\n"
            + "EMPNO=7782; a=1\n"
            + "EMPNO=7788; a=1\n"
            + "EMPNO=7839; a=1\n"
            + "EMPNO=7844; a=1\n"
            + "EMPNO=7876; a=1\n"
            + "EMPNO=7900; a=1\n"
            + "EMPNO=7902; a=1\n"
            + "EMPNO=7934; a=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT `EMPNO`, 1 `a`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEval2() {
    String ppl = "source=EMP | eval a = 1, b = 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[1], b=[2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1 `a`, 2 `b`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEval3() {
    String ppl = "source=EMP | eval a = 1 | eval b = 2 | eval c = 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[1], b=[2], c=[3])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1 `a`, 2 `b`,"
            + " 3 `c`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalWithSort() {
    String ppl = "source=EMP | eval a = EMPNO | sort - a | fields a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(a=[$8])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], a=[$0])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "a=7934\n"
            + "a=7902\n"
            + "a=7900\n"
            + "a=7876\n"
            + "a=7844\n"
            + "a=7839\n"
            + "a=7788\n"
            + "a=7782\n"
            + "a=7698\n"
            + "a=7654\n"
            + "a=7566\n"
            + "a=7521\n"
            + "a=7499\n"
            + "a=7369\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT `EMPNO` `a`\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalUsingExistingFields() {
    String ppl =
        "source=EMP | eval EMPNO_PLUS = EMPNO + 1 | sort - EMPNO_PLUS | fields EMPNO, EMPNO_PLUS |"
            + " head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], EMPNO_PLUS=[$8])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC], fetch=[3])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], EMPNO_PLUS=[+($0, 1)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "EMPNO=7934; EMPNO_PLUS=7935\n"
            + "EMPNO=7902; EMPNO_PLUS=7903\n"
            + "EMPNO=7900; EMPNO_PLUS=7901\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `EMPNO_PLUS`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `EMPNO` + 1 `EMPNO_PLUS`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 9 DESC NULLS FIRST\n"
            + "LIMIT 3) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalOverridingExistingFields() {
    String ppl =
        "source=EMP | eval SAL = DEPTNO + 10000 | sort - EMPNO | fields EMPNO, SAL | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], SAL=[$7])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC], fetch=[3])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " COMM=[$6], DEPTNO=[$7], SAL=[+($7, 10000)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "" + "EMPNO=7934; SAL=10010\n" + "EMPNO=7902; SAL=10020\n" + "EMPNO=7900; SAL=10030\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `DEPTNO` + 10000 `SAL`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexEvalCommands1() {
    String ppl =
        "source=EMP | eval col1 = 1 | sort col1 | head 4 | eval col2 = 2 | sort - col2 | sort EMPNO"
            + " | head 2 | fields EMPNO, ENAME, col2";
    RelNode root = getRelNode(ppl);
    String expectedResult =
        "" + "EMPNO=7369; ENAME=SMITH; col2=2\n" + "EMPNO=7499; ENAME=ALLEN; col2=2\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `col2`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `col1`, `col2`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1"
            + " `col1`, 2 `col2`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY '1' NULLS LAST\n"
            + "LIMIT 4) `t1`\n"
            + "ORDER BY `col2` DESC NULLS FIRST) `t2`\n"
            + "ORDER BY `EMPNO` NULLS LAST\n"
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexEvalCommands2() {
    String ppl =
        "source=EMP | eval col1 = SAL | sort - col1 | head 3 | eval col2 = SAL | sort + col2 |"
            + " fields ENAME, SAL | head 2";
    RelNode root = getRelNode(ppl);
    String expectedResult = "" + "ENAME=SCOTT; SAL=3000.00\n" + "ENAME=FORD; SAL=3000.00\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, `SAL`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `SAL` `col1`, `SAL` `col2`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL` DESC NULLS FIRST\n"
            + "LIMIT 3) `t1`\n"
            + "ORDER BY `col2` NULLS LAST\n"
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexEvalCommands3() {
    String ppl =
        "source=EMP | eval col1 = SAL | sort - col1 | head 3 | fields ENAME, col1 | eval col2 ="
            + " col1 | sort + col2 | fields ENAME, col2 | eval col3 = col2 | head 2 | fields ENAME,"
            + " col3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$0], col3=[$2])\n"
            + "  LogicalSort(sort0=[$2], dir0=[ASC], fetch=[2])\n"
            + "    LogicalProject(ENAME=[$1], col1=[$8], col2=[$8])\n"
            + "      LogicalSort(sort0=[$8], dir0=[DESC], fetch=[3])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], col1=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "" + "ENAME=SCOTT; col3=3000.00\n" + "ENAME=FORD; col3=3000.00\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`, `col2` `col3`\n"
            + "FROM (SELECT `ENAME`, `SAL` `col1`, `SAL` `col2`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL` DESC NULLS FIRST\n"
            + "LIMIT 3) `t1`\n"
            + "ORDER BY `col2` NULLS LAST\n"
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexEvalCommands4() {
    String ppl =
        "source=EMP | eval col1 = SAL | sort - col1 | head 3 | fields ENAME, col1 | eval col2 ="
            + " col1 | sort + col2 | fields ENAME, col2 | eval col3 = col2 | head 2 | fields"
            + " HIREDATE, col3";
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              RelNode root = getRelNode(ppl);
            });
    assertThat(
        e.getMessage(), is("field [HIREDATE] not found; input fields are: [ENAME, col2, col3]"));
  }

  @Test
  public void testEvalWithAggregation() {
    String ppl = "source=EMP | eval a = SAL, b = DEPTNO | stats avg(a) by b";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(a)=[$1], b=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{1}], avg(a)=[AVG($0)])\n"
            + "      LogicalProject(a=[$5], b=[$7])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg(a)=2916.66; b=10\navg(a)=2175.00; b=20\navg(a)=1566.66; b=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`SAL`) `avg(a)`, `DEPTNO` `b`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDependedEval() {
    String ppl = "source=EMP | eval a = SAL | eval b = a + 10000 | stats avg(b) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(b)=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{0}], avg(b)=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], b=[+($5, 10000)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(b)=12916.66; DEPTNO=10\n"
            + "avg(b)=12175.00; DEPTNO=20\n"
            + "avg(b)=11566.66; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`SAL` + 10000) `avg(b)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDependedLateralEval() {
    String ppl = "source=EMP | eval a = SAL, b = a + 10000 | stats avg(b) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(b)=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{0}], avg(b)=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], b=[+($5, 10000)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(b)=12916.66; DEPTNO=10\n"
            + "avg(b)=12175.00; DEPTNO=20\n"
            + "avg(b)=11566.66; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`SAL` + 10000) `avg(b)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
