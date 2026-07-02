/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Plan-shape tests for the {@code collect} command (no cluster): verify {@code visitCollect} builds
 * a pass-through {@link org.apache.calcite.rel.logical.LogicalTableSpool}, stamps options via a
 * Project, treats testmode as a no-spool dry run, and refuses missing/dot-prefixed destinations.
 */
public class CalcitePPLCollectTest extends CalcitePPLAbstractTest {

  public CalcitePPLCollectTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /**
   * Bare collect builds a LogicalTableSpool over the scan (pass-through write node). This both
   * verifies LogicalTableSpool is constructible on the plugin's Calcite version and pins the shape.
   */
  @Test
  public void testCollectBuildsLogicalTableSpool() {
    RelNode root = getRelNode("source=EMP | collect index=DEPT");

    assertTrue(
        "collect must build Calcite's pass-through Spool node, not a rowcount TableModify",
        root instanceof TableSpool);
    TableSpool spool = (TableSpool) root;
    assertEquals("[scott, DEPT]", spool.getTable().getQualifiedName().toString());
    // Pass-through: the spool's output row type equals its input (the scanned EMP rows).
    assertEquals(root.getInput(0).getRowType(), root.getRowType());

    String expectedLogical =
        "LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  /**
   * collect after a filter is the FULL TABLE SCOPE shape (scan + pushed-down filter, nothing
   * changing rows): the reindex fast lane keys on exactly this shape.
   */
  @Test
  public void testCollectWithFilterIsFullTableScopeShape() {
    RelNode root = getRelNode("source=EMP | where DEPTNO = 10 | collect index=DEPT");

    assertTrue(root instanceof TableSpool);
    String expectedLogical =
        "LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[scott, DEPT]])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  /**
   * collect after stats is the NON-full-scope shape (an aggregation sits above the scan), which
   * cannot be expressed as a reindex query and must take the batched-bulk path.
   */
  @Test
  public void testCollectAfterStatsIsNonScanShape() {
    RelNode root = getRelNode("source=EMP | stats count() by DEPTNO | collect index=DEPT");

    assertTrue(root instanceof TableSpool);
    String plan = root.explain();
    assertTrue(
        "collect after stats must carry an aggregation in its input subtree (non-full-scope)",
        plan.contains("LogicalAggregate"));
  }

  /**
   * Pre-existence: a destination index that does not resolve must fail at plan time with a clear
   * semantic error naming the index (SPL requires a pre-existing destination), NOT a raw NPE.
   */
  @Test
  public void testCollectMissingDestinationFailsAtPlanTime() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> getRelNode("source=EMP | collect index=NONEXISTENT_DEST"));
    assertTrue(
        "error must name the missing destination index, was: " + e.getMessage(),
        e.getMessage().contains("NONEXISTENT_DEST"));
  }

  /** System/hidden indices (dot-prefixed) are refused as collect destinations at plan time. */
  @Test
  public void testCollectDotIndexRefusedAtPlanTime() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> getRelNode("source=EMP | collect index=.system_index"));
    assertTrue(
        "error must flag the system/hidden index refusal, was: " + e.getMessage(),
        e.getMessage().contains("system or hidden"));
  }

  /**
   * The source/host/sourcetype/marker options stamp literal columns onto every row via a Project
   * below the spool; those columns are both written and passed through (SingleRel row type).
   */
  @Test
  public void testCollectStampsOptionColumns() {
    RelNode root =
        getRelNode("source=EMP | collect index=DEPT source='app1' host='h1' marker='m1'");

    assertTrue(root instanceof TableSpool);
    RelNode input = root.getInput(0);
    assertTrue(
        "stamps must be appended via a Project below the spool",
        input instanceof org.apache.calcite.rel.logical.LogicalProject);
    assertTrue(input.getRowType().getFieldNames().contains("source"));
    assertTrue(input.getRowType().getFieldNames().contains("host"));
    assertTrue(input.getRowType().getFieldNames().contains("marker"));
    // Pass-through: the spool surfaces the stamped columns too (row type = input row type).
    assertTrue(root.getRowType().getFieldNames().contains("source"));
    assertTrue(root.getRowType().getFieldNames().contains("marker"));
  }

  /** testmode is a dry-run: the plan carries no write node (spool), only the (stamped) rows. */
  @Test
  public void testCollectTestmodeSkipsWrite() {
    RelNode root = getRelNode("source=EMP | collect index=DEPT testmode=true");
    assertFalse(
        "testmode must not build a write spool: " + root.explain(), root instanceof TableSpool);
  }

  /** testmode still applies the stamps so the result previews what would be written. */
  @Test
  public void testCollectTestmodeStillStamps() {
    RelNode root = getRelNode("source=EMP | collect index=DEPT testmode=true source='app1'");
    assertFalse(root instanceof TableSpool);
    assertTrue(
        "testmode must still stamp the preview rows",
        root.getRowType().getFieldNames().contains("source"));
  }

  /** testmode does not require the destination to resolve, since it performs no write. */
  @Test
  public void testCollectTestmodeSkipsPreExistenceCheck() {
    RelNode root = getRelNode("source=EMP | collect index=NONEXISTENT_DEST testmode=true");
    assertFalse(root instanceof TableSpool);
  }
}
