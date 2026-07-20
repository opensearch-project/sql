/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Logical-plan tests for the {@code multikv} command (fixed-schema).
 *
 * <p>multikv lowers to a three-layer rewrite: an {@code eval} that calls {@code MULTIKV_SPLIT} into
 * a helper record column, an mvexpand (Uncollect over a Correlate) that explodes one row per table
 * data row, and a per-declared-column {@code MULTIKV_EXTRACT} eval followed by a narrowing project.
 * These assertions pin the presence and absence of those operators rather than the full Rex
 * rendering, which is exercised end-to-end by CalcitePPLMultikvCommandIT.
 */
public class CalcitePPLMultikvTest extends CalcitePPLAbstractTest {
  public CalcitePPLMultikvTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  private String plan(String ppl) {
    RelNode root = getRelNode(ppl);
    return RelOptUtil.toString(root);
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
  public void testMultikvFieldsSingleColumn() {
    String p = plan("source=EMP | eval _raw = ENAME | multikv fields pctIdle | fields pctIdle");
    // parse layer
    assertTrue(p, p.contains("MULTIKV_SPLIT"));
    // explode layer (mvexpand = Uncollect over Correlate)
    assertTrue(p, p.contains("Uncollect") || p.contains("Correlate"));
    // extract layer for the declared column
    assertTrue(p, p.contains("MULTIKV_EXTRACT"));
    assertTrue(p, p.contains("pctIdle"));
  }

  @Test
  public void testMultikvFieldsMultipleColumns() {
    String p = plan("source=EMP | eval _raw = ENAME | multikv fields CPU pctIdle");
    // one extract per declared column
    int idx = p.indexOf("MULTIKV_EXTRACT");
    assertTrue(p, idx >= 0 && p.indexOf("MULTIKV_EXTRACT", idx + 1) >= 0);
    assertTrue(p, p.contains("CPU"));
    assertTrue(p, p.contains("pctIdle"));
  }

  @Test
  public void testMultikvForceHeader() {
    // forceheader=2 is threaded into MULTIKV_SPLIT as the header-line argument.
    String p = plan("source=EMP | eval _raw = ENAME | multikv fields endpoint forceheader=2");
    assertTrue(p, p.contains("MULTIKV_SPLIT"));
    assertTrue(p, p.contains("MULTIKV_EXTRACT"));
    assertTrue(p, p.contains("endpoint"));
  }

  @Test
  public void testMultikvNoHeaderIsRowExplosionOnly() {
    // Positional noheader with no fields: split + explode, but no named-column extract/project.
    String p = plan("source=EMP | eval _raw = ENAME | multikv noheader=true");
    assertTrue(p, p.contains("MULTIKV_SPLIT"));
    assertTrue(p, p.contains("Uncollect") || p.contains("Correlate"));
    assertFalse(p, p.contains("MULTIKV_EXTRACT"));
  }

  @Test
  public void testBareMultikvRejectedWithGuidance() {
    // Bare auto-header multikv (no fields, no noheader) cannot resolve a plan-time schema and is
    // rejected at the field-resolution phase with actionable guidance.
    expectError("source=EMP | eval _raw = ENAME | multikv", "fields clause");
  }
}
