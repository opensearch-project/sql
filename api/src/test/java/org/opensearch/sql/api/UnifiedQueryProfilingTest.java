/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.monitor.profile.MetricName.EXECUTE;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfiling;

/** Tests for profiling across unified query components and the measure() API. */
public class UnifiedQueryProfilingTest extends UnifiedQueryTestBase {

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    return super.contextBuilder().profiling(true);
  }

  @Test
  public void testProfilingEnabled() {
    assertTrue(QueryProfiling.current().isEnabled());
  }

  @Test
  public void testProfilingDisabledByDefault() throws Exception {
    try (UnifiedQueryContext ctx = super.contextBuilder().build()) {
      assertFalse(QueryProfiling.current().isEnabled());
    }
  }

  @Test
  public void testGetProfileReturnsEmptyWhenDisabled() throws Exception {
    try (UnifiedQueryContext ctx = super.contextBuilder().build()) {
      assertFalse(ctx.getProfile().isPresent());
    }
  }

  @Test
  public void testMeasureExecutesWhenProfilingDisabled() throws Exception {
    try (UnifiedQueryContext ctx = super.contextBuilder().build()) {
      assertEquals("done", ctx.measure(EXECUTE, () -> "done"));
      assertFalse(ctx.getProfile().isPresent());
    }
  }

  @Test
  public void testPlannerAutoProfilesAnalyzePhase() {
    planner.plan("source = catalog.employees");
    assertTrue(context.getProfile().get().getPhases().get("analyze").getTimeMillis() >= 0);
  }

  @Test
  public void testCompilerAutoProfilesOptimizePhase() {
    RelNode plan = planner.plan("source = catalog.employees");
    new UnifiedQueryCompiler(context).compile(plan);
    assertTrue(context.getProfile().get().getPhases().get("optimize").getTimeMillis() >= 0);
  }

  @Test
  public void testMeasureRecordsMetric() throws Exception {
    assertEquals("done", context.measure(EXECUTE, () -> "done"));
    assertTrue(context.getProfile().get().getPhases().get("execute").getTimeMillis() >= 0);
  }

  @Test
  public void testFullPipelineProfiling() throws Exception {
    RelNode plan = planner.plan("source = catalog.employees");
    PreparedStatement stmt = new UnifiedQueryCompiler(context).compile(plan);
    ResultSet rs = context.measure(EXECUTE, stmt::executeQuery);

    QueryProfile profile = context.getProfile().get();
    assertTrue(profile.getSummary().getTotalTimeMillis() >= 0);
    assertTrue(profile.getPhases().get("analyze").getTimeMillis() >= 0);
    assertTrue(profile.getPhases().get("optimize").getTimeMillis() >= 0);
    assertTrue(profile.getPhases().get("execute").getTimeMillis() >= 0);
    assertNotNull(profile.getPlan());
  }

  @Test
  public void testProfilingClearedAfterClose() throws Exception {
    assertTrue(QueryProfiling.current().isEnabled());
    context.close();
    assertFalse(QueryProfiling.current().isEnabled());
  }

  @Test
  public void testMeasurePropagatesException() {
    assertThrows(
        IOException.class,
        () ->
            context.measure(
                EXECUTE,
                () -> {
                  throw new IOException("test error");
                }));
  }
}
