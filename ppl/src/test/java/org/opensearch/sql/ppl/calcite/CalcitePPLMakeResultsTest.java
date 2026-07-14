/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

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
        "LogicalProject(name=[CAST($0):VARCHAR NOT NULL], age=[CAST($1):BIGINT NOT NULL],"
            + " score=[CAST($2):REAL NOT NULL])\n"
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
        "LogicalProject(n=[CAST($0):VARCHAR NOT NULL])\n"
            + "  LogicalValues(tuples=[[{ '99999999999999999999' }]])\n");
  }

  @Test
  public void testMakeResultsRejectsNestedJson() {
    expectError("makeresults format=json data='[{\"a\":{\"x\":1}}]'", "nested JSON");
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
    // T1: count beyond the codegen-safe bound (5000) is rejected cleanly, not with an opaque
    // "Code grows beyond 64 KB" codegen failure.
    expectError("makeresults count=6000", "must not exceed 5000");
  }

  @Test
  public void testMakeResultsRejectsCountOverflow() {
    // T3: a count outside int range yields a clean validation error, not a raw
    // NumberFormatException.
    expectError("makeresults count=99999999999999", "not a valid integer");
  }
}
