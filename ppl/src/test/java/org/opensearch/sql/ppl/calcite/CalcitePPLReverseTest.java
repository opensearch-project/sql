/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLReverseTest extends CalcitePPLAbstractTest {
  public CalcitePPLReverseTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testReverseParserSuccess() {
    String ppl = "source=EMP | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __reverse_row_num__=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__reverse_row_num__`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 9 DESC NULLS FIRST) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseWithSortParserSuccess() {
    String ppl = "source=EMP | sort ENAME | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __reverse_row_num__=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__reverse_row_num__`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `ENAME`) `t0`\n"
            + "ORDER BY `__reverse_row_num__` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDoubleReverseParserSuccess() {
    String ppl = "source=EMP | reverse | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __reverse_row_num__=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalSort(sort0=[$8], dir0=[DESC])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __reverse_row_num__=[ROW_NUMBER() OVER ()])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__reverse_row_num__`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__reverse_row_num__`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 9 DESC NULLS FIRST) `t0`\n"
            + "ORDER BY 9 DESC NULLS FIRST) `t2`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseWithHeadParserSuccess() {
    String ppl = "source=EMP | reverse | head 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC], fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __reverse_row_num__=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__reverse_row_num__`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 9 DESC NULLS FIRST\n"
            + "LIMIT 2) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = Exception.class)
  public void testReverseWithNumberShouldFail() {
    String ppl = "source=EMP | reverse 2";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReverseWithFieldShouldFail() {
    String ppl = "source=EMP | reverse EMPNO";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReverseWithStringShouldFail() {
    String ppl = "source=EMP | reverse \"desc\"";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReverseWithExpressionShouldFail() {
    String ppl = "source=EMP | reverse EMPNO + 1";
    getRelNode(ppl);
  }
}
