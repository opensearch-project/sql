/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLHighlightTest extends CalcitePPLAbstractTest {

  public CalcitePPLHighlightTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testHighlightWildcard() {
    String ppl = "source=EMP | highlight *";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalHighlight(highlightArgs=[[*]])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHighlightSingleTerm() {
    String ppl = "source=EMP | highlight \"error\"";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalHighlight(highlightArgs=[[error]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHighlightMultipleTerms() {
    String ppl = "source=EMP | highlight \"error\", \"login\"";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalHighlight(highlightArgs=[[error, login]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHighlightWithFilter() {
    String ppl = "source=EMP | highlight * | where SAL > 2000";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[>($5, 2000)])\n"
            + "  LogicalHighlight(highlightArgs=[[*]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHighlightWithFields() {
    String ppl = "source=EMP | highlight \"error\" | fields ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], _highlight=[$8])\n"
            + "  LogicalHighlight(highlightArgs=[[error]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHighlightWithSort() {
    String ppl = "source=EMP | highlight * | sort SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "  LogicalHighlight(highlightArgs=[[*]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHighlightWithFilterAndFields() {
    String ppl = "source=EMP | highlight \"error\" | where SAL > 2000 | fields ENAME, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], SAL=[$5], _highlight=[$8])\n"
            + "  LogicalFilter(condition=[>($5, 2000)])\n"
            + "    LogicalHighlight(highlightArgs=[[error]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }
}
