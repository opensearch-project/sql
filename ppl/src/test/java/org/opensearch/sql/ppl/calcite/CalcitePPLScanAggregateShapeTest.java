/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.calcite.utils.ScanAggregates;

/** Which PPL queries group a scan more than once -- the shape whose aggregates share one subset. */
public class CalcitePPLScanAggregateShapeTest extends CalcitePPLAbstractTest {

  public CalcitePPLScanAggregateShapeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  private void assertSeveral(String ppl) {
    assertTrue(ppl, ScanAggregates.moreThanOneGroupedOverAScan(getRelNode(ppl)));
  }

  private void assertAtMostOne(String ppl) {
    assertFalse(ppl, ScanAggregates.moreThanOneGroupedOverAScan(getRelNode(ppl)));
  }

  @Test
  public void chartGroupsTwiceOverOneScan() {
    // The charted rows, plus the pass ranking the top-N columns on a different key.
    assertSeveral("source=EMP | chart count() over DEPTNO by JOB");
  }

  @Test
  public void timechartGroupsTwiceOverOneScan() {
    assertSeveral("source=EMP | timechart timefield=HIREDATE span=1month count() by JOB");
  }

  @Test
  public void appendOverAnAggregatingSubsearch() {
    assertSeveral(
        "source=EMP | stats count() by DEPTNO | append [ source=EMP | stats count() by JOB ]");
  }

  @Test
  public void appendcolOverAnAggregatingSubsearch() {
    assertSeveral("source=EMP | stats count() by DEPTNO | appendcol [ stats count() by JOB ]");
  }

  @Test
  public void multisearchOverAggregatingSubsearches() {
    assertSeveral(
        "| multisearch [ search source=EMP | stats count() by DEPTNO ] [ search source=EMP | stats"
            + " count() by JOB ]");
  }

  @Test
  public void inSubqueryOverAnAggregatingSubsearch() {
    assertSeveral(
        "source=EMP | stats count() by DEPTNO | where DEPTNO in [ source=EMP | stats count() by"
            + " DEPTNO | fields DEPTNO ]");
  }

  @Test
  public void plainStatsGroupsOnce() {
    assertAtMostOne("source=EMP | stats count() by DEPTNO");
  }

  @Test
  public void chainedStatsSecondAggregateIsUngrouped() {
    assertAtMostOne("source=EMP | stats count() as c by DEPTNO | stats sum(c) as total");
  }

  @Test
  public void chartWithoutTopNRankingGroupsOnce() {
    assertAtMostOne("source=EMP | chart limit=0 count() over DEPTNO by JOB");
  }

  /** A second, ungrouped aggregate partitions nothing, so these keep partial mode. */
  @Test
  public void anUngroupedSecondAggregateDoesNotCount() {
    assertAtMostOne("source=EMP | stats sum(SAL) as s by DEPTNO | addcoltotals s");
    assertAtMostOne("source=EMP | stats count() as c by DEPTNO | streamstats window=2 sum(c)");
  }

  /** Exotic plan shapes must read as one aggregate, not trip the walk into failing closed. */
  @Test
  public void singleAggregateSurvivesExoticNeighbours() {
    assertAtMostOne("source=EMP | dedup JOB | stats count() by DEPTNO");
    assertAtMostOne("source=EMP | eventstats avg(SAL) as a by DEPTNO | stats count() by JOB");
    assertAtMostOne("source=EMP | stats count() by DEPTNO | trendline sma(2, `count()`)");
    assertAtMostOne("source=EMP | top 2 JOB");
    assertAtMostOne("source=EMP | rare 2 JOB");
    assertAtMostOne("source=EMP | bin SAL span=1000 | stats count() by SAL");
    assertAtMostOne("source=EMP | streamstats count() as c | stats max(c)");
    assertAtMostOne("source=EMP | sort SAL | head 5 | stats count() by JOB");
  }

  @Test
  public void noAggregateAtAll() {
    assertAtMostOne("source=EMP | where DEPTNO = 20 | fields ENAME");
  }
}
