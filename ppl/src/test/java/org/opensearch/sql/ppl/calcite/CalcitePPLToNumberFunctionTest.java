/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLToNumberFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLToNumberFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testNumberBinary() {
    String ppl = "source=EMP | eval int_value = tonumber('010101',2) | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(int_value=[TONUMBER('010101':VARCHAR, 2)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=21.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('010101', 2) `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberBinaryUnsupportedResultNull() {
    String ppl = "source=EMP | eval int_value = tonumber('010.101',2) | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(int_value=[TONUMBER('010.101':VARCHAR, 2)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=null\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('010.101', 2) `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberHex() {
    String ppl = "source=EMP | eval int_value = tonumber('FA34',16) | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(int_value=[TONUMBER('FA34':VARCHAR, 16)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=64052.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('FA34', 16) `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberHexUnsupportedValuesResultNull() {
    String ppl =
        "source=EMP | eval double_value = tonumber('FA.34',16) | fields double_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(double_value=[TONUMBER('FA.34':VARCHAR, 16)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "double_value=null\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('FA.34', 16) `double_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberHexMinLimit() {
    String ppl =
        "source=EMP | eval long_value = tonumber('-7FFFFFFFFFFFFFFF',16) | fields long_value|head"
            + " 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(long_value=[TONUMBER('-7FFFFFFFFFFFFFFF':VARCHAR, 16)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "long_value=-9.223372036854776E18\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('-7FFFFFFFFFFFFFFF', 16) `long_value`\nFROM `scott`.`EMP`\nLIMIT 1";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberHexMaxLimit() {
    String ppl =
        "source=EMP | eval long_value = tonumber('7FFFFFFFFFFFFFFF',16) | fields long_value|head"
            + " 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(long_value=[TONUMBER('7FFFFFFFFFFFFFFF':VARCHAR, 16)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "long_value=9.223372036854776E18\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('7FFFFFFFFFFFFFFF', 16) `long_value`\nFROM `scott`.`EMP`\nLIMIT 1";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberHexOverNegativeMaxLimit() {
    String ppl =
        "source=EMP | eval long_value = tonumber('-FFFFFFFFFFFFFFFF',16) | fields long_value|head"
            + " 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(long_value=[TONUMBER('-FFFFFFFFFFFFFFFF':VARCHAR, 16)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "long_value=1.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('-FFFFFFFFFFFFFFFF', 16) `long_value`\nFROM `scott`.`EMP`\nLIMIT 1";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberHexOverPositiveMaxLimit() {
    String ppl =
        "source=EMP | eval long_value = tonumber('FFFFFFFFFFFFFFFF',16) | fields long_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(long_value=[TONUMBER('FFFFFFFFFFFFFFFF':VARCHAR, 16)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "long_value=-1.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('FFFFFFFFFFFFFFFF', 16) `long_value`\nFROM `scott`.`EMP`\nLIMIT 1";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumber() {
    String ppl = "source=EMP | eval int_value = tonumber('4598') | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(int_value=[TONUMBER('4598':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=4598.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT TONUMBER('4598') `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberDecimal() {
    String ppl = "source=EMP | eval int_value = tonumber('4598.54922') | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(int_value=[TONUMBER('4598.54922':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=4598.54922\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('4598.54922') `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNumberUnsupportedResultNull() {
    String ppl = "source=EMP | eval int_value = tonumber('4A598.54922') | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(int_value=[TONUMBER('4A598.54922':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=null\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TONUMBER('4A598.54922') `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
