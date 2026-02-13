/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLNoMvTest extends CalcitePPLAbstractTest {

  private static final String LS = System.lineSeparator();

  public CalcitePPLNoMvTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /**
   * Override to avoid normalizing the '\n' delimiter inside ARRAY_JOIN. The base class's
   * normalization replaces ALL \n with System.lineSeparator(), which incorrectly changes the
   * delimiter from '\n' to '\r\n' on Windows. The delimiter should always be '\n' regardless of
   * platform - it's a data value, not a line separator.
   */
  @Override
  public void verifyPPLToSparkSQL(RelNode rel, String expected) {
    // Don't normalize - expect strings are written with explicit System.lineSeparator()
    SqlImplementor.Result result = getConverter().visitRoot(rel);
    final SqlNode sqlNode = result.asStatement();
    final String sql = sqlNode.toSqlString(OpenSearchSparkSqlDialect.DEFAULT).getSql();
    org.hamcrest.MatcherAssert.assertThat(sql, org.hamcrest.CoreMatchers.is(expected));
  }

  // Helper to access converter from parent
  private RelToSqlConverter getConverter() {
    return new RelToSqlConverter(OpenSearchSparkSqlDialect.DEFAULT);
  }

  @Test
  public void testNoMvBasic() {
    String ppl =
        "source=EMP | eval arr = array('web', 'production', 'east') | nomv arr | head 1 | fields"
            + " arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('web':VARCHAR, 'production':VARCHAR,"
            + " 'east':VARCHAR)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('web', 'production', 'east')), '\n"
            + "'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvMultipleDocuments() {
    String ppl =
        "source=EMP | eval arr = array('web', 'production') | nomv arr | head 2 | fields"
            + " EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('web':VARCHAR, 'production':VARCHAR)),"
            + " '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('web', 'production')), '\n"
            + "'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvWithMultipleFields() {
    String ppl =
        "source=EMP | eval arr1 = array('a', 'b'), arr2 = array('x', 'y') | nomv arr1 | nomv arr2 |"
            + " head 1 | fields arr1, arr2";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(arr1=[$8], arr2=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " arr1=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('a', 'b')), '\n"
            + "'), '':VARCHAR)], arr2=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('x', 'y')), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('a', 'b')), '\n"
            + "'), '') `arr1`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('x', 'y')), '\n"
            + "'), '') `arr2`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvWithArrayFromFields() {
    String ppl =
        "source=EMP | eval tags = array(ENAME, JOB) | nomv tags | head 1 | fields EMPNO, tags";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], tags=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], tags=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array($1,"
            + " $2)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(`ENAME`, `JOB`)), '\n"
            + "'), '') `tags`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvInPipeline() {
    String ppl =
        "source=EMP | where DEPTNO = 10 | eval names = array(ENAME, JOB) | nomv names | head 1 |"
            + " fields EMPNO, names";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], names=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " names=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array($1, $2)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalFilter(condition=[=($7, 10)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(`ENAME`, `JOB`)), '\n"
            + "'), '') `names`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "WHERE `DEPTNO` = 10"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvNonExistentField() {
    String ppl = "source=EMP | eval arr = array('a', 'b') | nomv does_not_exist | head 1";

    Exception ex = assertThrows(Exception.class, () -> getRelNode(ppl));

    String msg = String.valueOf(ex.getMessage());
    org.junit.Assert.assertTrue(
        "Expected error message to mention missing field or type error. Actual: " + msg,
        msg.toLowerCase().contains("does_not_exist")
            || msg.toLowerCase().contains("field")
            || msg.contains("ARRAY_COMPACT")
            || msg.contains("ARRAY"));
  }

  @Test
  public void testNoMvScalarFieldError() {
    String ppl = "source=EMP | nomv EMPNO | head 1";

    Exception ex = assertThrows(Exception.class, () -> getRelNode(ppl));

    String msg = String.valueOf(ex.getMessage());
    org.junit.Assert.assertTrue(
        "Expected error for non-array field. Actual: " + msg,
        msg.toLowerCase().contains("array") || msg.toLowerCase().contains("type"));
  }

  @Test
  public void testNoMvNonDirectFieldReferenceError() {
    String ppl = "source=EMP | eval arr = array('a', 'b') | nomv upper(arr) | head 1";

    Exception ex = assertThrows(Exception.class, () -> getRelNode(ppl));

    String msg = String.valueOf(ex.getMessage());
    org.junit.Assert.assertTrue(
        "Expected parser error for non-direct field reference. Actual: " + msg,
        msg.contains("(")
            || msg.toLowerCase().contains("syntax")
            || msg.toLowerCase().contains("parse"));
  }

  @Test
  public void testNoMvWithNestedArray() {
    String ppl =
        "source=EMP | eval arr = array('a', 'b', 'c') | nomv arr | eval arr_len = length(arr) |"
            + " head 1 | fields EMPNO, arr, arr_len";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8], arr_len=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('a',"
            + " 'b', 'c')), '\n"
            + "'), '':VARCHAR)], arr_len=[CHAR_LENGTH(COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('a',"
            + " 'b', 'c')), '\n"
            + "'), '':VARCHAR))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('a', 'b', 'c')), '\n"
            + "'), '') `arr`, CHAR_LENGTH(COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('a', 'b', 'c')),"
            + " '\n"
            + "'), '')) `arr_len`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvWithConcatInArray() {
    String ppl =
        "source=EMP | eval full_name = concat(ENAME, ' - ', JOB), arr = array(full_name) | nomv"
            + " arr | head 1 | fields EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], full_name=[CONCAT($1, ' - ':VARCHAR, $2)],"
            + " arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array(CONCAT($1, ' - ':VARCHAR, $2))), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(CONCAT(`ENAME`, ' - ', `JOB`))),"
            + " '\n"
            + "'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvSingleElementArray() {
    String ppl = "source=EMP | eval arr = array('single') | nomv arr | head 1 | fields EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('single':VARCHAR)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('single')), '\n'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvEmptyArray() {
    String ppl = "source=EMP | eval arr = array() | nomv arr | head 1 | fields EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array()),"
            + " '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY()), '\n'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvLargeArray() {
    String ppl =
        "source=EMP | eval arr = array('1', '2', '3', '4', '5', '6', '7', '8', '9', '10') | nomv"
            + " arr | head 1 | fields arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('1',"
            + " '2', '3', '4', '5', '6', '7', '8', '9', '10':VARCHAR)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('1', '2', '3', '4', '5', '6', '7', '8',"
            + " '9', '10')), '\n"
            + "'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvChainedWithOtherOperations() {
    String ppl =
        "source=EMP | eval arr = array('a', 'b') | nomv arr | eval arr_upper = upper(arr) | head"
            + " 1 | fields arr, arr_upper";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(arr=[$8], arr_upper=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('a',"
            + " 'b')), '\n"
            + "'), '':VARCHAR)], arr_upper=[UPPER(COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array('a',"
            + " 'b')), '\n"
            + "'), '':VARCHAR))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('a', 'b')), '\n"
            + "'), '') `arr`, UPPER(COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY('a', 'b')), '\n"
            + "'), '')) `arr_upper`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvWithNullableField() {
    String ppl =
        "source=EMP | eval arr = array(ENAME, COMM) | nomv arr | head 1 | fields EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array($1,"
            + " $6)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(`ENAME`, `COMM`)), '\n"
            + "'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvWithMultipleNullableFields() {
    String ppl = "source=EMP | eval arr = array(MGR, COMM) | nomv arr | head 1 | fields EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array($3,"
            + " $6)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(`MGR`, `COMM`)), '\n'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNoMvWithMixedNullableAndNonNullableFields() {
    String ppl =
        "source=EMP | eval arr = array(ENAME, COMM, JOB) | nomv arr | head 1 | fields EMPNO, arr";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], arr=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[COALESCE(ARRAY_JOIN(ARRAY_COMPACT(array($1,"
            + " $6, $2)), '\n"
            + "'), '':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(`ENAME`, `COMM`, `JOB`)), '\n"
            + "'), '') `arr`"
            + LS
            + "FROM `scott`.`EMP`"
            + LS
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
