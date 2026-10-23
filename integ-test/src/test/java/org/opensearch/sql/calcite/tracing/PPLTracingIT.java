/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tracing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.calcite.tracing.OtlpHttpTraceReceiver.Span;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.ClusterPlugins;

public class PPLTracingIT extends PPLIntegTestCase {

  private static final Duration SPAN_WAIT_TIMEOUT = Duration.ofSeconds(15);

  private static final int KIND_INTERNAL = 1;
  private static final int KIND_CLIENT = 3;

  private static final int STATUS_UNSET = 0;
  private static final int STATUS_ERROR = 2;

  private static OtlpHttpTraceReceiver receiver;

  @BeforeClass
  public static void startReceiver() throws IOException {
    int port = Integer.parseInt(System.getProperty("tests.tracing.otlp.port", "4318"));
    receiver = new OtlpHttpTraceReceiver(port);
  }

  @AfterClass
  public static void stopReceiver() {
    if (receiver != null) receiver.close();
  }

  @Override
  public void init() throws Exception {
    super.init();
    ClusterPlugins.requirePluginOrAssume(
        client(),
        ClusterPlugins.TELEMETRY_OTEL_PLUGIN,
        "telemetry-otel plugin not installed on test cluster; skipping PPL tracing tests");
    enableCalcite();
    loadIndex(Index.BANK);
    receiver.clear();
  }

  @Test
  public void executeQuery_emitsRootAndFourPhaseSpans() throws Exception {
    executePPL("source=" + TEST_INDEX_BANK + " | where age > 30 | stats count() by state");

    Span root = waitForRoot();
    assertThat(root.kind, is(KIND_CLIENT));
    assertThat(root.attributes, hasKey("db.system.name"));
    assertThat(root.attr("db.system.name"), equalTo("opensearch"));
    assertThat(root.attr("db.query.type"), equalTo("ppl"));
    assertThat(root.attr("db.operation.name"), equalTo("EXECUTE"));
    assertThat(root.attr("db.query.id"), is(notNullValue()));
    assertThat(root.attr("db.query.text"), startsWith("source="));
    assertThat(root.statusCode, is(not(STATUS_ERROR)));

    assertThat(
        phaseChildNames(root),
        equalTo(
            Set.of(
                "opensearch.query.prepare",
                "opensearch.query.analyze",
                "opensearch.query.optimize",
                "opensearch.query.execute")));

    for (Span child : phaseChildrenOf(root)) {
      assertThat(child.kind, is(KIND_INTERNAL));
      assertThat(child.durationNanos(), greaterThan(-1L));
    }
  }

  @Test
  public void explainQuery_emitsPrepareAnalyzeOptimizeButNoExecute() throws Exception {
    Request req = new Request("POST", "/_plugins/_ppl/_explain");
    req.setJsonEntity("{\"query\":\"source=" + TEST_INDEX_BANK + " | stats count() by state\"}");
    client().performRequest(req);

    Span root = waitForRoot(s -> "EXPLAIN".equals(s.attr("db.operation.name")));

    assertThat(root.attr("db.operation.name"), equalTo("EXPLAIN"));
    Set<String> phases = phaseChildNames(root);
    assertThat(
        phases,
        equalTo(
            Set.of(
                "opensearch.query.prepare",
                "opensearch.query.analyze",
                "opensearch.query.optimize")));
    assertThat(phases.contains("opensearch.query.execute"), is(false));
  }

  @Test
  public void parseCommand_emitsAllFourPhases_evenThroughComplexPoolHop() throws Exception {
    executePPL(
        "source="
            + TEST_INDEX_BANK
            + " | parse address '(?<num>\\\\d+)' | where isnotnull(num) | stats count() by num");

    Span root = waitForRoot();
    assertThat(
        phaseChildNames(root),
        equalTo(
            Set.of(
                "opensearch.query.prepare",
                "opensearch.query.analyze",
                "opensearch.query.optimize",
                "opensearch.query.execute")));
    assertThat(phaseChildrenOf(root).size(), is(4));
  }

  @Test
  public void failedQueryOnMissingIndex_setsStatusErrorOnRoot() throws Exception {
    try {
      Request req = new Request("POST", "/_plugins/_ppl");
      req.setJsonEntity("{\"query\":\"source=this-index-does-not-exist-xyz | stats count()\"}");
      client().performRequest(req);
      throw new AssertionError("expected ResponseException for missing index");
    } catch (ResponseException expected) {
    }

    Span root = waitForRoot();
    if (root.statusCode != STATUS_ERROR) {
      throw new AssertionError(dumpDiagnostics("expected root.statusCode=ERROR", root));
    }
    Set<String> phases = phaseChildNames(root);
    assertThat(phases.contains("opensearch.query.execute"), is(false));
  }

  @Test
  public void anonymizedQueryTextIsNotRawUserInput() throws Exception {
    executePPL("source=" + TEST_INDEX_BANK + " | where age > 30 | fields firstname");

    Span root = waitForRoot();
    String qtext = root.attr("db.query.text");
    assertThat(qtext, is(notNullValue()));
    assertThat(qtext.contains(" 30"), is(false));
    assertThat(qtext.contains("***"), is(true));
  }

  @Test
  public void phaseSpans_haveValidTimingAndSameTraceIdAsRoot() throws Exception {
    executePPL("source=" + TEST_INDEX_BANK + " | stats count()");

    Span root = waitForRoot();
    List<Span> phases = phaseChildrenOf(root);
    assertThat(phases.size(), greaterThan(0));
    for (Span p : phases) {
      assertThat(p.traceId, equalTo(root.traceId));
      assertThat(p.parentSpanId, equalTo(root.spanId));
      assertThat(p.endEpochNanos, greaterThan(p.startEpochNanos - 1));
      assertThat(
          p.startEpochNanos >= root.startEpochNanos && p.endEpochNanos <= root.endEpochNanos,
          is(true));
    }
  }

  private void executePPL(String query) throws IOException {
    Request req = new Request("POST", "/_plugins/_ppl");
    req.setJsonEntity("{\"query\":\"" + query.replace("\"", "\\\"").replace("\\", "\\\\") + "\"}");
    client().performRequest(req);
  }

  private Span waitForRoot() throws InterruptedException {
    return waitForRoot(s -> true);
  }

  /**
   * Wait for a root span matching {@code extra}, then keep polling until ALL {@code
   * opensearch.query*} spans (roots + phase children) stop arriving. Phase children have different
   * names than the root, so a root-only stability check can return before the batch carrying its
   * children has drained — callers of {@link #phaseChildrenOf} would then see a partial set. Tests
   * run sequentially; the latest-created matching root is this test's.
   */
  private Span waitForRoot(Predicate<Span> extra) throws InterruptedException {
    Predicate<Span> isRoot = s -> "opensearch.query".equals(s.name) && extra.test(s);
    Predicate<Span> isAnyPplSpan = s -> s.name.startsWith("opensearch.query");
    receiver.waitForSpans(isRoot, 1, SPAN_WAIT_TIMEOUT);
    int cur = (int) receiver.snapshot().stream().filter(isAnyPplSpan).count();
    int prev;
    do {
      prev = cur;
      Thread.sleep(100);
      cur = (int) receiver.snapshot().stream().filter(isAnyPplSpan).count();
    } while (cur > prev);
    return receiver.snapshot().stream()
        .filter(isRoot)
        .reduce((a, b) -> a.startEpochNanos >= b.startEpochNanos ? a : b)
        .orElseThrow();
  }

  private String dumpDiagnostics(String header, Span pickedRoot) {
    StringBuilder sb = new StringBuilder(header).append("\n");
    sb.append("  picked root: ")
        .append(pickedRoot)
        .append(" attrs=")
        .append(pickedRoot.attributes)
        .append("\n  all opensearch.query* spans in receiver:");
    receiver.snapshot().stream()
        .filter(s -> s.name.startsWith("opensearch.query"))
        .forEach(
            s ->
                sb.append("\n    ")
                    .append(
                        String.format(
                            "name=%s trace=%s span=%s parent=%s status=%d msg=%s attrs=%s",
                            s.name,
                            s.traceId,
                            s.spanId,
                            s.parentSpanId,
                            s.statusCode,
                            s.statusMessage,
                            s.attributes)));
    return sb.toString();
  }

  private List<Span> phaseChildrenOf(Span root) {
    return receiver.snapshot().stream()
        .filter(s -> s.name.startsWith("opensearch.query."))
        .filter(s -> root.spanId.equals(s.parentSpanId))
        .collect(Collectors.toList());
  }

  private Set<String> phaseChildNames(Span root) {
    return phaseChildrenOf(root).stream().map(s -> s.name).collect(Collectors.toSet());
  }
}
