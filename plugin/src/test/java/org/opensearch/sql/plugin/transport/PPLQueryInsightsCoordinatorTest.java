/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.ppl.QueryInsightsMetadata;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * What {@link PPLQueryInsightsCoordinator} decides on its own: whether a query is recorded, what
 * its record keeps about the query, how the query ended, and which finished queries are reported.
 * {@link TransportPPLQueryActionQueryInsightsTest} drives the record end to end through the action.
 */
public class PPLQueryInsightsCoordinatorTest {

  /** Registered by the Query Insights plugin, so absent when it is not installed. */
  private static final Setting<Boolean> RECORDING =
      Setting.boolSetting(
          PPLQueryInsightsCoordinator.RECORDING_ENABLED_KEY,
          false,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  private final ClusterService clusterService = mock(ClusterService.class);
  private final PPLQueryInsightsCoordinator queryInsights =
      new PPLQueryInsightsCoordinator(
          clusterService, mock(TransportService.class), mock(ThreadPool.class));

  @Test
  public void recordsOnlyAPplTaskWithTheSettingOn() {
    PPLQueryTask task = newTask();

    recording(null);
    assertFalse("Query Insights not installed", queryInsights.isEnabledFor(task));
    recording(false);
    assertFalse(queryInsights.isEnabledFor(task));
    recording(true);
    assertTrue(queryInsights.isEnabledFor(task));
    assertFalse(
        "a task another plugin's copy of the SQL classes created",
        queryInsights.isEnabledFor(null));
  }

  @Test
  public void aFailedSettingsLookupLeavesRecordingOff() {
    when(clusterService.getClusterSettings()).thenThrow(new IllegalStateException("boom"));
    assertFalse(queryInsights.isEnabledFor(newTask()));
  }

  @Test
  public void sinkKeepsTheQueryAsWrittenAndWhetherItIsAnExplain() {
    PPLQueryTask task = newTask();

    queryInsights
        .sink(task, "source=t | where a > 30")
        .accept(new QueryInsightsMetadata("source=t | where a > ***", false));
    assertEquals(
        "the query as written, not anonymized",
        "source=t | where a > 30",
        task.getQueryInsightsQueryText());
    assertFalse(task.isQueryInsightsExplain());

    queryInsights.sink(task, "explain source=t").accept(new QueryInsightsMetadata("x", true));
    assertTrue(task.isQueryInsightsExplain());
  }

  @Test
  public void sinkWithoutATaskDoesNothing() {
    queryInsights.sink(null, "source=t").accept(new QueryInsightsMetadata("source=t", false));
  }

  @Test
  public void completionNotesTheLatencyAndWhetherTheQueryFailed() {
    AtomicReference<Object> outcome = new AtomicReference<>();

    PPLQueryTask answered = newTask();
    TransportPPLQueryResponse response = new TransportPPLQueryResponse("{}");
    queryInsights.recordCompletion(listener(outcome), answered).onResponse(response);
    assertSame(response, outcome.get());
    assertTrue(answered.getQueryInsightsLatencyMillis() >= 0);
    assertFalse(answered.isQueryInsightsFailed());

    PPLQueryTask failed = newTask();
    IllegalStateException boom = new IllegalStateException("boom");
    queryInsights.recordCompletion(listener(outcome), failed).onFailure(boom);
    assertSame(boom, outcome.get());
    assertTrue(failed.getQueryInsightsLatencyMillis() >= 0);
    assertTrue(failed.isQueryInsightsFailed());
  }

  @Test
  public void completionWithoutATaskKeepsTheListener() {
    ActionListener<TransportPPLQueryResponse> listener = listener(new AtomicReference<>());
    assertSame(listener, queryInsights.recordCompletion(listener, null));
  }

  @Test
  public void testInlineExplainIsNotReported() {
    // "explain source=t | ..." reaches the execute route because isExplainRequest() only looks at
    // the path, so the sink's flag is what keeps it out of Top N.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("explain source=table | fields identifier");
    task.setQueryInsightsExplain(true);
    assertFalse(PPLQueryInsightsCoordinator.shouldReport(task));
  }

  @Test
  public void testSyntaxErrorIsNotReported() {
    // A parse failure throws before the sink runs, leaving no query text to report.
    assertFalse(PPLQueryInsightsCoordinator.shouldReport(newTask()));
  }

  @Test
  public void testRuntimeFailureAfterPlanningIsReported() {
    // Planning succeeded, so the metadata is present; a later execution failure must still report.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("source=table | where identifier > 42");
    task.setQueryInsightsFailed(true);
    assertTrue(PPLQueryInsightsCoordinator.shouldReport(task));
  }

  @Test
  public void testNestedQueryIsNotReported() {
    // An outer marker was already in the context, so the child searches rolled up into that query.
    // Reporting here would advertise a marker no child references and double count their cost.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("source=table | where identifier > 42");
    task.setQueryInsightsNested(true);
    assertFalse(PPLQueryInsightsCoordinator.shouldReport(task));
  }

  @Test
  public void testNonNestedQueryIsReportedByDefault() {
    // Guards the default: a plain query must not be mistaken for a nested one.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("source=table | where identifier > 42");
    assertFalse("nested must default to false", task.isQueryInsightsNested());
    assertTrue(PPLQueryInsightsCoordinator.shouldReport(task));
  }

  private void recording(Boolean enabled) {
    ClusterSettings settings =
        enabled == null
            ? new ClusterSettings(Settings.EMPTY, Set.of())
            : new ClusterSettings(
                Settings.builder().put(RECORDING.getKey(), enabled).build(),
                Set.<Setting<?>>of(RECORDING));
    when(clusterService.getClusterSettings()).thenReturn(settings);
  }

  private static ActionListener<TransportPPLQueryResponse> listener(
      AtomicReference<Object> outcome) {
    return ActionListener.wrap(outcome::set, outcome::set);
  }

  private static PPLQueryTask newTask() {
    return new PPLQueryTask(
        1,
        "transport",
        "cluster:admin/opensearch/ppl",
        "test query",
        TaskId.EMPTY_TASK_ID,
        Map.of());
  }
}
