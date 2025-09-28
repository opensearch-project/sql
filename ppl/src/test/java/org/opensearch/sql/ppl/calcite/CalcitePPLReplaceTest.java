/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.Collections;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Replace;

public class CalcitePPLReplaceTest extends CalcitePPLAbstractTest {

  public CalcitePPLReplaceTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testBasicReplace() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], new_JOB=[REPLACE($2, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "REPLACE(`JOB`, 'CLERK', 'EMPLOYEE') `new_JOB`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);

    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; new_JOB=EMPLOYEE\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; new_JOB=SALESMAN\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; new_JOB=SALESMAN\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; new_JOB=MANAGER\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; new_JOB=SALESMAN\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; new_JOB=MANAGER\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; new_JOB=MANAGER\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; new_JOB=ANALYST\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; new_JOB=PRESIDENT\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; new_JOB=SALESMAN\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; new_JOB=EMPLOYEE\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; new_JOB=EMPLOYEE\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; new_JOB=ANALYST\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; new_JOB=EMPLOYEE\n";

    verifyResult(root, expectedResult);
  }

  @Test
  public void testMultipleFieldsReplace() {
    String ppl =
        "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB | replace \"20\" WITH \"RESEARCH\""
            + " IN DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], new_JOB=[REPLACE($2, 'CLERK':VARCHAR, 'EMPLOYEE':VARCHAR)],"
            + " new_DEPTNO=[REPLACE($7, '20':VARCHAR, 'RESEARCH':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "REPLACE(`JOB`, 'CLERK', 'EMPLOYEE') `new_JOB`, "
            + "REPLACE(`DEPTNO`, '20', 'RESEARCH') `new_DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceSameValueInMultipleFields() {
    // In EMP table, both JOB and MGR fields contain numeric values
    String ppl = "source=EMP | replace \"7839\" WITH \"CEO\" IN MGR, EMPNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], new_MGR=[REPLACE($3, '7839':VARCHAR, 'CEO':VARCHAR)],"
            + " new_EMPNO=[REPLACE($0, '7839':VARCHAR, 'CEO':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "REPLACE(`MGR`, '7839', 'CEO') `new_MGR`, "
            + "REPLACE(`EMPNO`, '7839', 'CEO') `new_EMPNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithPipeline() {
    String ppl =
        "source=EMP | where JOB = 'CLERK' | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB | sort SAL";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], new_JOB=[REPLACE($2, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR)])\n"
            + "    LogicalFilter(condition=[=($2, 'CLERK':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "REPLACE(`JOB`, 'CLERK', 'EMPLOYEE') `new_JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `JOB` = 'CLERK'\n"
            + "ORDER BY `SAL`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = Exception.class)
  public void testReplaceWithoutWithKeywordShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" \"EMPLOYEE\" IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReplaceWithoutInKeywordShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" JOB";
    getRelNode(ppl);
  }

  @Test(expected = RuntimeException.class)
  public void testReplaceWithExpressionShouldFail() {
    String ppl = "source=EMP | replace EMPNO + 1 WITH \"EMPLOYEE\" IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReplaceWithInvalidFieldShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN INVALID_FIELD";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReplaceWithMultipleInKeywordsShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB IN ENAME";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReplaceWithMissingQuotesShouldFail() {
    String ppl = "source=EMP | replace CLERK WITH EMPLOYEE IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReplaceWithMissingReplacementValueShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithNullPatternShouldFail() {
    Replace replace =
        new Replace(null, new Literal("EMPLOYEE", DataType.STRING), Collections.emptyList());
    replace.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithNullReplacementShouldFail() {
    Replace replace =
        new Replace(new Literal("CLERK", DataType.STRING), null, Collections.emptyList());
    replace.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithNonStringPatternShouldFail() {
    Replace replace =
        new Replace(
            new Literal(123, DataType.INTEGER),
            new Literal("EMPLOYEE", DataType.STRING),
            Collections.emptyList());
    replace.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithNonStringReplacementShouldFail() {
    Replace replace =
        new Replace(
            new Literal("CLERK", DataType.STRING),
            new Literal(456, DataType.INTEGER),
            Collections.emptyList());
    replace.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithNullFieldListShouldFail() {
    Replace replace =
        new Replace(
            new Literal("CLERK", DataType.STRING), new Literal("EMPLOYEE", DataType.STRING), null);
    replace.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithEmptyFieldListShouldFail() {
    Replace replace =
        new Replace(
            new Literal("CLERK", DataType.STRING),
            new Literal("EMPLOYEE", DataType.STRING),
            Collections.emptyList());
    replace.validate();
  }
}
