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

public class CalcitePPLFieldFormatTest extends CalcitePPLAbstractTest {

  public CalcitePPLFieldFormatTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testFieldFormat1() {
    String ppl = "source=EMP | sort EMPNO| head 3 | fieldformat a = 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; a=1\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; a=1\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; a=1\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1 `a`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormat2() {
    String ppl =
        "source=EMP | sort EMPNO| head 3 |fieldformat formatted_salary ="
            + " \"$\".tostring(SAL,\"commas\")";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], formatted_salary=[||('$':VARCHAR, TOSTRING($5,"
            + " 'commas':VARCHAR))])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; formatted_salary=$800\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; formatted_salary=$1,600\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; formatted_salary=$1,250\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, '$' ||"
            + " TOSTRING(`SAL`, 'commas') `formatted_salary`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormatAndFields() {
    String ppl =
        "source=EMP | sort EMPNO| head 5|fieldformat formatted_salary ="
            + " \"$\".tostring(SAL,\"commas\") |fields EMPNO, JOB, formatted_salary";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], JOB=[$2], formatted_salary=[||('$':VARCHAR, TOSTRING($5,"
            + " 'commas':VARCHAR))])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; JOB=CLERK; formatted_salary=$800\n"
            + "EMPNO=7499; JOB=SALESMAN; formatted_salary=$1,600\n"
            + "EMPNO=7521; JOB=SALESMAN; formatted_salary=$1,250\n"
            + "EMPNO=7566; JOB=MANAGER; formatted_salary=$2,975\n"
            + "EMPNO=7654; JOB=SALESMAN; formatted_salary=$1,250\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `JOB`, '$' || TOSTRING(`SAL`, 'commas') `formatted_salary`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 5";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormat2Fields() {
    String ppl = "source=EMP | sort EMPNO| head 3 | fieldformat a = 1, b = 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[1], b=[2])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; a=1; b=2\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; a=1; b=2\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; a=1; b=2\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1 `a`, 2 `b`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormat3() {
    String ppl =
        "source=EMP | sort EMPNO| head 3 | fieldformat a = 1 | fieldformat b = 2 | fieldformat c ="
            + " 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[1], b=[2], c=[3])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; a=1; b=2; c=3\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; a=1; b=2; c=3\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; a=1; b=2; c=3\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 1 `a`, 2 `b`,"
            + " 3 `c`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormatSum() {
    String ppl =
        "source=EMP |sort EMPNO | head 3| fieldformat total = sum(1, 2, 3) | fields EMPNO, total";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], total=[+(1, +(2, 3))])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult = "EMPNO=7369; total=6\nEMPNO=7499; total=6\nEMPNO=7521; total=6\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, 1 + (2 + 3) `total`\nFROM `scott`.`EMP`\nORDER BY `EMPNO`\nLIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormatWithToNumber() {
    String ppl =
        "source=EMP | sort EMPNO | head 3| fieldformat total = sum(SAL, COMM, 100) | fieldformat"
            + " total =  \"$\".cast(total as string) | fields EMPNO, total";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], total=[||('$':VARCHAR, NUMBER_TO_STRING(+($5, +($6, 100))))])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; total=null\nEMPNO=7499; total=$2000.00\nEMPNO=7521; total=$1850.00\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, '$' || NUMBER_TO_STRING(`SAL` + (`COMM` + 100)) `total`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexFieldFormatCommands4() {
    String ppl =
        "source=EMP | fieldformat col1 = SAL | sort - col1 | head 3 | fields ENAME, col1 |"
            + " fieldformat col2 = col1 | sort + col2 | fields ENAME, col2 | fieldformat col3 ="
            + " col2 | head 2 | fields HIREDATE, col3";
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              RelNode root = getRelNode(ppl);
            });
    assertThat(e.getMessage(), is("Field [HIREDATE] not found."));
  }

  @Test
  public void testFieldFormatMaxOnStrings() {
    String ppl =
        "source=EMP | sort EMPNO | head 3 |fieldformat a = \"Max String:\".max('banana', 'Door',"
            + " ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], a=[||('Max String:':VARCHAR, SCALAR_MAX('banana':VARCHAR,"
            + " 'Door':VARCHAR, $1))])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; a=Max String:banana\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; a=Max String:banana\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; a=Max String:banana\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, 'Max String:'"
            + " || SCALAR_MAX('banana', 'Door', `ENAME`) `a`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormatMaxOnStringsWithSuffix() {
    String ppl =
        "source=EMP | sort EMPNO | head 3 |fields EMPNO, ENAME| fieldformat a = max('banana',"
            + " 'Door', ENAME).\" after comparing with provided constant strings and ENAME column"
            + " values.\"";
    // #
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], a=[||(SCALAR_MAX('banana':VARCHAR, 'Door':VARCHAR,"
            + " $1), ' after comparing with provided constant strings and ENAME column"
            + " values.':VARCHAR)])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; a=banana after comparing with provided constant strings and ENAME"
            + " column values.\n"
            + "EMPNO=7499; ENAME=ALLEN; a=banana after comparing with provided constant strings and"
            + " ENAME column values.\n"
            + "EMPNO=7521; ENAME=WARD; a=banana after comparing with provided constant strings and"
            + " ENAME column values.\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, SCALAR_MAX('banana', 'Door', `ENAME`) || ' after comparing with"
            + " provided constant strings and ENAME column values.' `a`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldFormatMaxOnStringsWithPrefixSuffix() {
    String ppl =
        "source=EMP | sort EMPNO | head 3 |fields EMPNO, ENAME| fieldformat a = \"Max String:"
            + " \\\"\".max('banana', 'Door', ENAME).\"\\\" after comparing with provided constant"
            + " strings and ENAME column values.\"";
    // #
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], a=[||(||('Max String: \"':VARCHAR,"
            + " SCALAR_MAX('banana':VARCHAR, 'Door':VARCHAR, $1)), '\" after comparing with"
            + " provided constant strings and ENAME column values.':VARCHAR)])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; a=Max String: \"banana\" after comparing with provided constant"
            + " strings and ENAME column values.\n"
            + "EMPNO=7499; ENAME=ALLEN; a=Max String: \"banana\" after comparing with provided"
            + " constant strings and ENAME column values.\n"
            + "EMPNO=7521; ENAME=WARD; a=Max String: \"banana\" after comparing with provided"
            + " constant strings and ENAME column values.\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, 'Max String: \"' || SCALAR_MAX('banana', 'Door', `ENAME`) || '\""
            + " after comparing with provided constant strings and ENAME column values.' `a`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalMinOnNumericAndString() {
    String ppl =
        "source=EMP  | sort EMPNO | head 3| fields EMPNO, ENAME, DEPTNO | fieldformat a = \"Minimum"
            + " of DEPTNO, ENAME and Provided list of  5, 30, 'banana', 'Door': \".min(5, 30,"
            + " DEPTNO, 'banana', 'Door', ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7], a=[||('Minimum of DEPTNO, ENAME and"
            + " Provided list of  5, 30, ''banana'', ''Door'': ':VARCHAR, SCALAR_MIN(5, 30, $7,"
            + " 'banana':VARCHAR, 'Door':VARCHAR, $1))])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[3])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; DEPTNO=20; a=Minimum of DEPTNO, ENAME and Provided list of  5,"
            + " 30, 'banana', 'Door': 5\n"
            + "EMPNO=7499; ENAME=ALLEN; DEPTNO=30; a=Minimum of DEPTNO, ENAME and Provided list of "
            + " 5, 30, 'banana', 'Door': 5\n"
            + "EMPNO=7521; ENAME=WARD; DEPTNO=30; a=Minimum of DEPTNO, ENAME and Provided list of "
            + " 5, 30, 'banana', 'Door': 5\n";

    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `DEPTNO`, 'Minimum of DEPTNO, ENAME and Provided list of  5, 30,"
            + " ''banana'', ''Door'': ' || SCALAR_MIN(5, 30, `DEPTNO`, 'banana', 'Door', `ENAME`)"
            + " `a`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO`\n"
            + "LIMIT 3";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
