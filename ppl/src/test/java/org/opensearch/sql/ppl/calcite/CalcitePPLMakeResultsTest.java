/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/** Logical-plan tests for the makeresults leading command (count path + format/data path). */
public class CalcitePPLMakeResultsTest extends CalcitePPLAbstractTest {
  public CalcitePPLMakeResultsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  private void expectError(String ppl, String messageFragment) {
    try {
      getRelNode(ppl);
      fail("expected an error for: " + ppl);
    } catch (Exception e) {
      String msg = String.valueOf(e.getMessage());
      assertTrue(
          "expected message containing '" + messageFragment + "' but got: " + msg,
          msg.contains(messageFragment));
    }
  }

  @Test
  public void testMakeResultsBare() {
    RelNode root = getRelNode("makeresults");
    verifyLogical(root, "LogicalProject(@timestamp=[NOW()])\n  LogicalValues(tuples=[[{ 0 }]])\n");
  }

  @Test
  public void testMakeResultsCount() {
    RelNode root = getRelNode("makeresults count=3");
    verifyLogical(
        root,
        "LogicalProject(@timestamp=[NOW()])\n"
            + "  LogicalValues(tuples=[[{ 0 }, { 1 }, { 2 }]])\n");
  }

  @Test
  public void testMakeResultsCountZero() {
    RelNode root = getRelNode("makeresults count=0");
    verifyLogical(root, "LogicalValues(tuples=[[]])\n");
  }

  @Test
  public void testMakeResultsJson() {
    RelNode root =
        getRelNode(
            "makeresults format=json data='[{\"name\":\"John\",\"age\":35,\"score\":3.5},"
                + "{\"name\":\"Sarah\",\"age\":39,\"score\":4.0}]'");
    verifyLogical(
        root,
        "LogicalProject(@timestamp=[NOW()], name=[CAST($0):VARCHAR NOT NULL],"
            + " age=[CAST($1):BIGINT NOT NULL], score=[CAST($2):REAL NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ 'John', 35, 3.5E0 }, { 'Sarah', 39, 4.0E0 }]])\n");
  }

  @Test
  public void testMakeResultsTypedCsv() {
    RelNode root =
        getRelNode("makeresults format=csv data='name:string,age:int\nJohn,35\nSarah,39'");
    verifyLogical(
        root,
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], age=[$1])\n"
            + "  LogicalValues(tuples=[[{ 'John', 35 }, { 'Sarah', 39 }]])\n");
  }

  @Test
  public void testMakeResultsBareCsv() {
    RelNode root = getRelNode("makeresults format=csv data='name,age\nJohn,35\nSarah,39'");
    verifyLogical(
        root,
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], age=[CAST($1):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ 'John', '35' }, { 'Sarah', '39' }]])\n");
  }

  @Test
  public void testMakeResultsHeaderOnlyCsv() {
    RelNode root = getRelNode("makeresults format=csv data='name,age'");
    verifyLogical(root, "LogicalValues(tuples=[[]])\n");
  }

  @Test
  public void testMakeResultsCsvQuotedComma() {
    RelNode root = getRelNode("makeresults format=csv data='name,note\nJohn,\"a,b\"'");
    verifyLogical(
        root,
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], note=[CAST($1):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ 'John', 'a,b' }]])\n");
  }

  @Test
  public void testMakeResultsJsonBigIntKeepsPrecision() {
    RelNode root = getRelNode("makeresults format=json data='[{\"n\":99999999999999999999}]'");
    verifyLogical(
        root,
        "LogicalProject(@timestamp=[NOW()], n=[CAST($0):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ '99999999999999999999' }]])\n");
  }

  @Test
  public void testMakeResultsSerializesNestedJson() {
    RelNode root = getRelNode("makeresults format=json data='[{\"a\":{\"x\":1},\"b\":[1,2]}]'");
    verifyLogical(
        root,
        "LogicalProject(@timestamp=[NOW()], a=[CAST($0):VARCHAR NOT NULL],"
            + " b=[CAST($1):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ '{\"x\":1}', '[1,2]' }]])\n");
  }

  @Test
  public void testMakeResultsRejectsAllNullColumn() {
    expectError("makeresults format=json data='[{\"a\":null},{\"a\":null}]'", "only null values");
  }

  @Test
  public void testMakeResultsRejectsDataWithoutFormat() {
    expectError("makeresults data='[{\"a\":1}]'", "format and data must be provided together");
  }

  @Test
  public void testMakeResultsNegativeCountYieldsZeroRows() {
    // A negative count silently yields zero rows rather than an error.
    RelNode root = getRelNode("makeresults count=-1");
    verifyLogical(root, "LogicalValues(tuples=[[]])\n");
  }

  @Test
  public void testMakeResultsRejectsCountOverCap() {
    // count > 5000 is rejected cleanly, not with a Janino 64 KB codegen failure.
    expectError("makeresults count=6000", "must not exceed 5000");
  }

  @Test
  public void testMakeResultsRejectsCellBudget() {
    // 50 columns x 120 rows = 6000 cells > 5000; a flat row cap would miss this.
    expectError(csvData(50, 120), "cells (rows x columns)");
  }

  @Test
  public void testMakeResultsAllowsAtCellBudget() {
    // 50 columns x 100 rows = 5000 cells is exactly at budget and must be accepted.
    assertNotNull(getRelNode(csvData(50, 100)));
  }

  @Test
  public void testMakeResultsRejectsOversizedCellValue() {
    // A single value over the 60000 per-value guard is rejected, not a codegen error.
    String wide = "x".repeat(60001);
    expectError(
        "makeresults format=csv data='c0\n" + wide + "'", "cell value must not exceed 60000");
  }

  @Test
  public void testMakeResultsRejectsCsvRowWithMoreColumns() {
    expectError(
        "makeresults format=csv data='name,age\nJohn,35,extra'", "more columns than the header");
  }

  @Test
  public void testMakeResultsCsvRowWithFewerColumnsPadsNull() {
    RelNode root = getRelNode("makeresults format=csv data='name,age\nJohn,35\nSarah'");
    verifyLogical(
        root,
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], age=[CAST($1):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ 'John', '35' }, { 'Sarah', null }]])\n");
  }

  @Test
  public void testMakeResultsCountAtCap() {
    assertNotNull(getRelNode("makeresults count=5000"));
  }

  @Test
  public void testMakeResultsAllowsCellValueAtLimit() {
    assertNotNull(getRelNode("makeresults format=csv data='c0\n" + "x".repeat(60000) + "'"));
  }

  private static String csvData(int cols, int rows) {
    StringBuilder sb = new StringBuilder("makeresults format=csv data='");
    for (int j = 0; j < cols; j++) sb.append(j == 0 ? "" : ",").append("c").append(j);
    for (int r = 0; r < rows; r++) {
      sb.append("\n");
      for (int j = 0; j < cols; j++) sb.append(j == 0 ? "" : ",").append("1");
    }
    return sb.append("'").toString();
  }

  @Test
  public void testMakeResultsRejectsCountOverflow() {
    // T3: a count outside int range yields a clean validation error, not a raw
    // NumberFormatException.
    expectError("makeresults count=99999999999999", "not a valid integer");
  }

  @Test
  public void testMakeResultsJsonUserTimestampWins() {
    RelNode root = getRelNode("makeresults format=json data='[{\"@timestamp\":\"2020\",\"x\":1}]'");
    verifyLogical(
        root,
        "LogicalProject(@timestamp=[CAST($0):VARCHAR NOT NULL], x=[CAST($1):BIGINT NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ '2020', 1 }]])\n");
  }

  @Test
  public void testMakeResultsRejectsInvalidBoolean() {
    expectError(
        "makeresults format=csv data='active:boolean\nnot true'", "cannot parse \"not true\"");
  }

  @Test
  public void testMakeResultsAcceptsBooleanCaseInsensitive() {
    RelNode root = getRelNode("makeresults format=csv data='active:boolean\nTRUE\nFalse'");
    verifyLogical(root, "LogicalValues(tuples=[[{ true }, { false }]])\n");
  }

  @Test
  public void testMakeResultsRejectsUnterminatedQuote() {
    expectError("makeresults format=csv data='name\n\"unterminated'", "unterminated quoted field");
  }

  @Test
  public void testMakeResultsUniquifiesDuplicateCsvHeaders() {
    RelNode root = getRelNode("makeresults format=csv data='name,name\nJohn,Doe'");
    verifyLogical(
        root,
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], name0=[CAST($1):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ 'John', 'Doe' }]])\n");
  }

  @Test
  public void testMakeResultsRejectsBlankCsvHeader() {
    expectError("makeresults format=csv data=',field\n1,2'", "blank column name");
  }

  @Test
  public void testMakeResultsSkipsWhitespaceOnlyCsvLines() {
    RelNode root = getRelNode("makeresults format=csv data='name,age\nJohn,35\n   \nSarah,39'");
    verifyLogical(
        root,
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], age=[CAST($1):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ 'John', '35' }, { 'Sarah', '39' }]])\n");
  }
}
