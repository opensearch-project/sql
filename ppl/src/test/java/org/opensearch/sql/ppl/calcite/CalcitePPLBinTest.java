/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLBinTest extends CalcitePPLAbstractTest {

  public CalcitePPLBinTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testBinWithSpan() {
    String ppl = "source=EMP | bin SAL span=1000";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
            + "COMM=[$6], DEPTNO=[$7], SAL=[SPAN_BUCKET($5, 1000)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `COMM`, `DEPTNO`, `SPAN_BUCKET`(`SAL`,"
            + " 1000) `SAL`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinWithBins() {
    String ppl = "source=EMP | bin SAL bins=10";
    RelNode root = getRelNode(ppl);

    // Note: WIDTH_BUCKET uses window functions without ROWS UNBOUNDED PRECEDING in the actual
    // output
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
            + "COMM=[$6], DEPTNO=[$7], SAL=[WIDTH_BUCKET($5, 10, "
            + "-(MAX($5) OVER (), MIN($5) OVER ()), "
            + "MAX($5) OVER ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testBinWithMinspan() {
    String ppl = "source=EMP | bin SAL minspan=100";
    RelNode root = getRelNode(ppl);

    // Note: MINSPAN_BUCKET converts the minspan to DOUBLE and uses window functions without ROWS
    // clause
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
            + "COMM=[$6], DEPTNO=[$7], SAL=[MINSPAN_BUCKET($5, 100.0E0:DOUBLE, "
            + "-(MAX($5) OVER (), MIN($5) OVER ()), "
            + "MAX($5) OVER ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testBinWithStartEnd() {
    String ppl = "source=EMP | bin SAL start=1000 end=5000";
    RelNode root = getRelNode(ppl);

    // Note: RANGE_BUCKET uses window functions without ROWS UNBOUNDED PRECEDING
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
            + "COMM=[$6], DEPTNO=[$7], SAL=[RANGE_BUCKET($5, "
            + "MIN($5) OVER (), MAX($5) OVER (), "
            + "1000, 5000)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testBinWithTimeSpan() {
    String ppl = "source=products_temporal | bin SYS_START span=1h";
    RelNode root = getRelNode(ppl);

    // Time span binning generates FROM_UNIXTIME expression
    verifyLogical(
        root,
        "LogicalProject(ID=[$0], SUPPLIER=[$1], SYS_END=[$3],"
            + " SYS_START=[FROM_UNIXTIME(*(FLOOR(/(/(UNIX_TIMESTAMP($2), 3600), 1)), 3600))])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n");

    verifyPPLToSparkSQL(
        root,
        "SELECT `ID`, `SUPPLIER`, `SYS_END`, `FROM_UNIXTIME`(FLOOR(`UNIX_TIMESTAMP`(`SYS_START`) /"
            + " 3600 / 1) * 3600) `SYS_START`\n"
            + "FROM `scott`.`products_temporal`");
  }

  @Test
  public void testBinWithAligntime() {
    String ppl = "source=products_temporal | bin SYS_START span=1h aligntime=earliest";
    RelNode root = getRelNode(ppl);

    // Time span binning with aligntime generates the same expression as without aligntime for this
    // case
    verifyLogical(
        root,
        "LogicalProject(ID=[$0], SUPPLIER=[$1], SYS_END=[$3],"
            + " SYS_START=[FROM_UNIXTIME(*(FLOOR(/(/(UNIX_TIMESTAMP($2), 3600), 1)), 3600))])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n");

    verifyPPLToSparkSQL(
        root,
        "SELECT `ID`, `SUPPLIER`, `SYS_END`, `FROM_UNIXTIME`(FLOOR(`UNIX_TIMESTAMP`(`SYS_START`) /"
            + " 3600 / 1) * 3600) `SYS_START`\n"
            + "FROM `scott`.`products_temporal`");
  }

  @Test(expected = SemanticCheckException.class)
  public void testBinWithMinspanOnNonNumericField() {
    String ppl = "source=EMP | bin ENAME minspan=10";
    getRelNode(ppl); // Should throw SemanticCheckException
  }

  @Test(expected = SemanticCheckException.class)
  public void testBinWithSpanOnNonNumericField() {
    String ppl = "source=EMP | bin JOB span=5";
    getRelNode(ppl); // Should throw SemanticCheckException
  }

  @Test(expected = SemanticCheckException.class)
  public void testBinWithBinsOnNonNumericField() {
    String ppl = "source=EMP | bin ENAME bins=10";
    getRelNode(ppl); // Should throw SemanticCheckException
  }

  @Test(expected = SemanticCheckException.class)
  public void testBinWithStartEndOnNonNumericField() {
    String ppl = "source=EMP | bin JOB start=1 end=10";
    getRelNode(ppl); // Should throw SemanticCheckException
  }

  @Test(expected = SemanticCheckException.class)
  public void testBinDefaultOnNonNumericField() {
    String ppl = "source=EMP | bin ENAME";
    getRelNode(ppl); // Should throw SemanticCheckException
  }
}
