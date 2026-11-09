/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * The is-not-null filter that {@code stats count(field)} adds sits on the projection the aggregate
 * reads, so its reference must be the column's index in that projection's output. An input-side
 * index is out of range when the projection narrows, and names the wrong column when it does not.
 *
 * <p>EMP is {@code EMPNO $0, ENAME $1, JOB $2, MGR $3, HIREDATE $4, SAL $5, COMM $6, DEPTNO $7}.
 */
public class CalcitePPLCountFrameTest extends CalcitePPLAbstractTest {

  public CalcitePPLCountFrameTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /** COMM is $6 of the scan and $0 of the projection, so an input-side index is out of range. */
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

  /**
   * SAL sits at output position 6, so $6 stays in range and resolves to SAL while COMM is $5. Both
   * are DECIMAL, so nothing objects. The result stays 4 because count ignores nulls itself, so only
   * the plan shows which column the filter is about.
   */
  @Test
  public void testCountWhenTheStaleIndexStaysInRange() {
    String ppl =
        "source=EMP | fields EMPNO, ENAME, JOB, MGR, HIREDATE, COMM, SAL | stats count(COMM) as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
            + "  LogicalProject(COMM=[$5])\n"
            + "    LogicalFilter(condition=[IS NOT NULL($5)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " COMM=[$6], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, "c=4\n");
  }

  /** dc() takes the same filter, so it took the same out-of-range reference. */
  @Test
  public void testDistinctCountAfterNarrowingFields() {
    String ppl = "source=EMP | fields COMM | stats dc(COMM) as c";
    RelNode root = getRelNode(ppl);
    verifyResult(root, "c=4\n");
  }

  /** Two names for one column, which is what the input-side mapping is kept for. */
  @Test
  public void testCountOfAnAliasedColumn() {
    String ppl = "source=EMP | eval bonus = COMM | fields bonus | stats count(bonus) as c";
    RelNode root = getRelNode(ppl);
    verifyResult(root, "c=4\n");
  }
}
