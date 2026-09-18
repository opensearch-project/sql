/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * {@code stats count(field)} adds an is-not-null filter, and these pin the frame its reference is
 * expressed in.
 *
 * <p>The filter is stacked on whatever the builder is holding, so the reference has to address that
 * node's output. A {@code fields} command in front of the {@code stats} makes the difference
 * visible: the column's index in the projection's output is not its index in the projection's
 * input, and using the latter is out of range as soon as the projection is narrower than its input.
 */
public class CalcitePPLCountFrameTest extends CalcitePPLAbstractTest {

  public CalcitePPLCountFrameTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /** COMM is $6 of the scan but $0 of the projection the filter sits on. */
  @Test
  public void testCountAfterNarrowingFields() {
    String ppl = "source=EMP | fields COMM | stats count(COMM) as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($0)])\n"
            + "    LogicalProject(COMM=[$6])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, "c=4\n");
  }

  /** Two columns kept, so the index is in range either way but only one of them is COMM. */
  @Test
  public void testCountAfterReorderingFields() {
    String ppl = "source=EMP | fields SAL, COMM | stats count(COMM) as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
            + "  LogicalProject(COMM=[$1])\n"
            + "    LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "      LogicalProject(SAL=[$5], COMM=[$6])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, "c=4\n");
  }

  /** dc() takes the same filter, so it takes the same reference. */
  @Test
  public void testDistinctCountAfterNarrowingFields() {
    String ppl = "source=EMP | fields COMM | stats dc(COMM) as c";
    RelNode root = getRelNode(ppl);
    verifyResult(root, "c=4\n");
  }

  /**
   * Two names for one column, which is the case the index mapping exists for: it recognises them as
   * the same column so a single filter covers both counts.
   */
  @Test
  public void testCountOfAnAliasedColumn() {
    String ppl = "source=EMP | eval bonus = COMM | fields bonus | stats count(bonus) as c";
    RelNode root = getRelNode(ppl);
    verifyResult(root, "c=4\n");
  }
}
