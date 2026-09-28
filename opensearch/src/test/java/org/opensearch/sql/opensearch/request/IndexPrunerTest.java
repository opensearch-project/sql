/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.constantScoreQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class IndexPrunerTest {

  @Mock private NodeClient node;
  @Mock private ActionFuture<FieldCapabilitiesResponse> matchFuture;
  @Mock private ActionFuture<ResolveIndexAction.Response> resolveFuture;
  @Mock private ResolveIndexAction.Response resolveResponse;
  @Mock private ActionFuture<FieldCapabilitiesResponse> readableFuture;

  /** Resolved index names; all readable unless {@code whenUnreadable} removes some. */
  private String[] resolvedNames = new String[0];

  @Nested
  class FilterShapes {

    @ParameterizedTest(name = "{0}")
    @MethodSource("filters")
    void shouldSeeATimestampRangeOnlyWhereTheProbeCanUseIt(
        String shape, QueryBuilder filter, boolean expected) {
      assertEquals(expected, IndexPruner.containsTimeRange(filter, IMPLICIT_FIELD_TIMESTAMP));
    }

    private static Stream<Arguments> filters() {
      return Stream.of(
          arguments("bare range on the timestamp", timeRange(), true),
          arguments("range under must", boolQuery().must(timeRange()), true),
          arguments("range under filter", boolQuery().filter(timeRange()), true),
          arguments("range under should", boolQuery().should(timeRange()), true),
          arguments("range under constant_score", constantScoreQuery(timeRange()), true),
          arguments("no filter at all", null, false),
          arguments("filter carrying no range", queryStringQuery("error"), false),
          arguments("range on another field", rangeQuery("status").gte(200), false),
          arguments(
              "range under should beside another clause",
              boolQuery().should(timeRange()).should(rangeQuery("status").gte(200)),
              true),
          // A negated clause cannot prove a shard disjoint, so it must not count.
          arguments("range under must_not", boolQuery().mustNot(timeRange()), false));
    }
  }

  @Nested
  class GateRejections {

    @Test
    void shouldNotPruneWhenExpressionHasNoWildcard() {
      givenIndexExpression("logs-2024", timeRange()).shouldNotPrune().shouldNotProbe();
    }

    @Test
    void shouldNotPruneWhenFilterHasNoTimestampRange() {
      givenIndexExpression("logs-*", queryStringQuery("error")).shouldNotPrune().shouldNotProbe();
    }

    @Test
    void shouldNotPruneAnExpressionNamingARemoteCluster() {
      givenIndexExpression("logs-*,remote:logs-*", timeRange()).shouldNotPrune().shouldNotProbe();
    }
  }

  @Nested
  class PruningDecisions {

    @Test
    void shouldPruneToTheMatchingIndices() {
      givenIndexExpression(indices("logs-*", 3), timeRange())
          .whenMatching("logs-e", "logs-f")
          .shouldPruneTo("logs-e,logs-f");
    }

    @Test
    void shouldNotPruneWhenNoIndexMatches() {
      givenIndexExpression(indices("logs-*", 3), timeRange()).whenMatching().shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenEveryIndexMatches() {
      givenIndexExpression(indices("logs-*", 2), timeRange())
          .whenMatching("logs-a", "logs-b")
          .shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenMoreIndicesMatchThanResolved() {
      givenIndexExpression(indices("logs-*", 3), timeRange())
          .whenMatching("logs-a", "logs-b", "logs-c", "logs-d")
          .shouldNotPrune();
    }
  }

  @Nested
  class IndirectResolution {

    @Test
    void shouldNotPruneWhenExpressionResolvesToAnAlias() {
      givenIndexExpression(alias("logs-*"), timeRange())
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneWhenExpressionResolvesToADataStream() {
      givenIndexExpression(ds("logs-*"), timeRange()).shouldNotPrune().shouldNotProbeForMatches();
    }
  }

  @Nested
  class ProbeFailures {

    @Test
    void shouldNotPruneWhenTheResolveProbeFails() {
      givenIndexExpression(unresolvable("logs-*"), timeRange())
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneWhenTheMatchProbeFails() {
      givenIndexExpression(indices("logs-*", 3), timeRange())
          .whenMatchProbeFails()
          .shouldNotPrune();
    }
  }

  @Nested
  class ProbeRequest {

    @Test
    void shouldProbeWithTheFilterExpressionAndTimestampField() {
      QueryBuilder filter = timeRange();
      givenIndexExpression(indices("logs-*", 3), filter)
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");

      FieldCapabilitiesRequest probe = capturedMatchProbe();
      assertSame(filter, probe.indexFilter());
      assertArrayEquals(new String[] {"logs-*"}, probe.indices());
      assertEquals(
          "[@timestamp]|" + SearchRequest.DEFAULT_INDICES_OPTIONS,
          Arrays.toString(probe.fields()) + "|" + probe.indicesOptions());
    }

    @Test
    void shouldProbeEveryNameOfACommaSeparatedExpression() {
      givenIndexExpression(indices("logs-a-*,logs-b-*", 3), timeRange())
          .whenMatching("logs-a-1")
          .shouldPruneTo("logs-a-1");

      assertArrayEquals(new String[] {"logs-a-*", "logs-b-*"}, capturedMatchProbe().indices());
    }
  }

  @Nested
  class BoundsDrivenPruning {

    @Test
    void shouldPruneOnTheBoundsOwnRangeWithNoPushedDownFilter() {
      givenIndexExpression(indices("logs-*", 3), timeRange("@timestamp"), "@timestamp")
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
    }

    /** The picker's field is the index pattern's, which is not always {@code @timestamp}. */
    @Test
    void shouldProbeTheBoundsOwnTimeField() {
      givenIndexExpression(indices("logs-*", 3), timeRange("event_time"), "event_time")
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");

      assertArrayEquals(new String[] {"event_time"}, capturedMatchProbe().fields());
    }

    /** Forwarded untouched, so date math reaches the index's own parser. */
    @Test
    void shouldProbeWithTheFilterExactlyAsGiven() {
      QueryBuilder range = timeRange("@timestamp");
      givenIndexExpression(indices("logs-*", 3), range, "@timestamp")
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");

      assertSame(range, capturedMatchProbe().indexFilter());
    }

    @Test
    void shouldNotPruneAConcreteExpression() {
      givenIndexExpression("logs-2024", timeRange("@timestamp"), "@timestamp")
          .shouldNotPrune()
          .shouldNotProbe();
    }

    @Test
    void shouldNotPruneAnAlias() {
      givenIndexExpression(alias("logs-*"), timeRange("@timestamp"), "@timestamp")
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneADataStream() {
      givenIndexExpression(ds("logs-*"), timeRange("@timestamp"), "@timestamp")
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneWhenTheProbeFails() {
      givenIndexExpression(indices("logs-*", 3), timeRange("@timestamp"), "@timestamp")
          .whenMatchProbeFails()
          .shouldNotPrune();
    }
  }

  @Nested
  class UnreadableIndices {

    /** Field caps reports no failure for an unreadable index, so it looks like an empty one. */
    @Test
    void shouldKeepAnIndexNoProbeCanRead() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-b", "logs-down"), timeRange())
          .whenMatching("logs-a")
          .whenUnreadable("logs-down")
          .shouldPruneTo("logs-a,logs-down");
    }

    @Test
    void shouldPruneNormallyWhenEveryIndexIsSearchable() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-b", "logs-c"), timeRange())
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
    }

    @Test
    void shouldNotPruneWhenKeepingTheUnreadableIndexCoversEverything() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-down"), timeRange())
          .whenMatching("logs-a")
          .whenUnreadable("logs-down")
          .shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenNothingMatchedEvenWithAnUnreadableIndex() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-down"), timeRange())
          .whenMatching()
          .whenUnreadable("logs-down")
          .shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenTheReadabilityProbeFails() {
      Fixture fixture =
          givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-b", "logs-c"), timeRange())
              .whenMatching("logs-a");
      // Overrides whenMatching's readable default.
      when(readableFuture.actionGet(any(TimeValue.class))).thenThrow(new RuntimeException("boom"));
      fixture.shouldNotPrune();
    }

    /** Index-scoped roles lack cluster:monitor/state, so it must never be requested. */
    @Test
    void shouldNotNameAnIndexDeletedAfterResolving() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-b", "logs-gone"), timeRange())
          .whenMatching("logs-a")
          .whenUnreadable("logs-gone")
          .whenDeletedAfterResolving("logs-gone")
          .shouldPruneTo("logs-a");
    }

    @Test
    void shouldSendBothProbesBeforeWaitingOnEither() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-b"), timeRange())
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");

      InOrder order = inOrder(node, matchFuture, readableFuture);
      order.verify(node).fieldCaps(argThat(request -> request.indexFilter() != null));
      order.verify(node).fieldCaps(argThat(request -> request.indexFilter() == null));
      order.verify(matchFuture).actionGet(any(TimeValue.class));
      order.verify(readableFuture).actionGet(any(TimeValue.class));
    }

    @Test
    void shouldJudgeReadabilityWithoutAPrivilegedRequest() {
      givenIndexExpression(namedIndices("logs-*", "logs-a", "logs-b", "logs-c"), timeRange())
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
      verify(node, never()).execute(eq(ClusterStateAction.INSTANCE), any());
    }
  }

  private FieldCapabilitiesRequest capturedMatchProbe() {
    ArgumentCaptor<FieldCapabilitiesRequest> captor =
        ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
    verify(node, atLeastOnce()).fieldCaps(captor.capture());
    return captor.getAllValues().stream()
        .filter(request -> request.indexFilter() != null)
        .findFirst()
        .orElseThrow();
  }

  private static QueryBuilder timeRange() {
    return rangeQuery("@timestamp").gte("now-1d");
  }

  /** Date math, to show the probe carries the range as given rather than re-interpreting it. */
  private static QueryBuilder timeRange(String field) {
    return rangeQuery(field).gte("now-15m").lte("now");
  }

  /** What the resolve probe reports for an expression. */
  private record Resolution(String expression, String[] names, Shape shape) {
    private enum Shape {
      INDICES,
      ALIAS,
      DATA_STREAM,
      FAILURE
    }
  }

  private static Resolution indices(String expression, int indexCount) {
    return namedIndices(expression, generatedNames(indexCount));
  }

  private static String[] generatedNames(int count) {
    return IntStream.rangeClosed(1, count).mapToObj(i -> "resolved-" + i).toArray(String[]::new);
  }

  private static Resolution namedIndices(String expression, String... names) {
    return new Resolution(expression, names, Resolution.Shape.INDICES);
  }

  private static Resolution alias(String expression) {
    return new Resolution(expression, new String[0], Resolution.Shape.ALIAS);
  }

  private static Resolution ds(String expression) {
    return new Resolution(expression, new String[0], Resolution.Shape.DATA_STREAM);
  }

  private static Resolution unresolvable(String expression) {
    return new Resolution(expression, new String[0], Resolution.Shape.FAILURE);
  }

  /** An expression the gates reject, so nothing is ever resolved. */
  private Fixture givenIndexExpression(String expression, QueryBuilder filter) {
    return new Fixture(expression, filter, (String) null);
  }

  private Fixture givenIndexExpression(String expression, QueryBuilder filter, String timeField) {
    return new Fixture(expression, filter, timeField);
  }

  /**
   * Each shape stubs only what {@code prune} reads for it, because short-circuiting leaves the rest
   * unread and Mockito rejects a stub nobody uses.
   */
  private Fixture givenIndexExpression(Resolution resolution, QueryBuilder filter) {
    when(node.execute(eq(ResolveIndexAction.INSTANCE), any())).thenReturn(resolveFuture);
    switch (resolution.shape()) {
      case FAILURE ->
          when(resolveFuture.actionGet(any(TimeValue.class)))
              .thenThrow(new RuntimeException("boom"));
      case ALIAS -> {
        whenResolved();
        when(resolveResponse.getAliases())
            .thenReturn(List.of(mock(ResolveIndexAction.ResolvedAlias.class)));
      }
      case DATA_STREAM -> {
        whenResolved();
        when(resolveResponse.getAliases()).thenReturn(List.of());
        when(resolveResponse.getDataStreams())
            .thenReturn(List.of(mock(ResolveIndexAction.ResolvedDataStream.class)));
      }
      case INDICES -> {
        whenResolved();
        when(resolveResponse.getAliases()).thenReturn(List.of());
        when(resolveResponse.getDataStreams()).thenReturn(List.of());
        List<ResolveIndexAction.ResolvedIndex> indices = resolvedIndices(resolution.names());
        // Lenient: a probe that throws or matches nothing never reads the list.
        lenient().when(resolveResponse.getIndices()).thenReturn(indices);
        resolvedNames = resolution.names();
      }
    }
    return new Fixture(resolution.expression(), filter, (String) null);
  }

  private Fixture givenIndexExpression(
      Resolution resolution, QueryBuilder filter, String timeField) {
    givenIndexExpression(resolution, (QueryBuilder) null);
    return new Fixture(resolution.expression(), filter, timeField);
  }

  private void whenResolved() {
    // Lenient: whenDeletedAfterResolving restubs it.
    lenient().when(resolveFuture.actionGet(any(TimeValue.class))).thenReturn(resolveResponse);
  }

  private static List<ResolveIndexAction.ResolvedIndex> resolvedIndices(String... names) {
    List<ResolveIndexAction.ResolvedIndex> indices = new ArrayList<>();
    for (String name : names) {
      ResolveIndexAction.ResolvedIndex index = mock(ResolveIndexAction.ResolvedIndex.class);
      lenient().when(index.getName()).thenReturn(name);
      indices.add(index);
    }
    return indices;
  }

  private final class Fixture {

    private final IndexName original;
    private final QueryBuilder filter;
    private final String timeField;
    private IndexName result;

    Fixture(String expression, QueryBuilder filter, String timeField) {
      this.original = new IndexName(expression);
      this.filter = filter;
      this.timeField = timeField;
    }

    Fixture whenMatching(String... matching) {
      whenEveryIndexReadable();
      when(node.fieldCaps(argThat(request -> request != null && request.indexFilter() != null)))
          .thenReturn(matchFuture);
      when(matchFuture.actionGet(any(TimeValue.class)))
          .thenReturn(new FieldCapabilitiesResponse(matching, Collections.emptyMap()));
      return this;
    }

    private void whenEveryIndexReadable() {
      lenient()
          .when(
              node.fieldCaps(argThat(request -> request != null && request.indexFilter() == null)))
          .thenReturn(readableFuture);
      lenient()
          .when(readableFuture.actionGet(any(TimeValue.class)))
          .thenReturn(new FieldCapabilitiesResponse(resolvedNames, Collections.emptyMap()));
    }

    Fixture whenDeletedAfterResolving(String... deleted) {
      Set<String> remaining = new LinkedHashSet<>(Arrays.asList(resolvedNames));
      Arrays.asList(deleted).forEach(remaining::remove);
      ResolveIndexAction.Response reResolved = mock(ResolveIndexAction.Response.class);
      List<ResolveIndexAction.ResolvedIndex> indices =
          resolvedIndices(remaining.toArray(String[]::new));
      when(reResolved.getIndices()).thenReturn(indices);
      when(resolveFuture.actionGet(any(TimeValue.class))).thenReturn(resolveResponse, reResolved);
      return this;
    }

    Fixture whenUnreadable(String... unreadable) {
      Set<String> stillReadable = new LinkedHashSet<>(Arrays.asList(resolvedNames));
      Arrays.asList(unreadable).forEach(stillReadable::remove);
      lenient()
          .when(readableFuture.actionGet(any(TimeValue.class)))
          .thenReturn(
              new FieldCapabilitiesResponse(
                  stillReadable.toArray(String[]::new), Collections.emptyMap()));
      return this;
    }

    Fixture whenMatchProbeFails() {
      whenEveryIndexReadable();
      when(node.fieldCaps(argThat(request -> request != null && request.indexFilter() != null)))
          .thenReturn(matchFuture);
      when(matchFuture.actionGet(any(TimeValue.class))).thenThrow(new RuntimeException("boom"));
      return this;
    }

    Fixture shouldPruneTo(String expected) {
      assertEquals(new IndexName(expected), pruned());
      return this;
    }

    /** Equality rather than identity: a declined probe rebuilds the expression it hands back. */
    Fixture shouldNotPrune() {
      assertEquals(original, pruned());
      return this;
    }

    Fixture shouldNotProbe() {
      pruned();
      verify(node, never()).execute(eq(ResolveIndexAction.INSTANCE), any());
      verify(node, never()).fieldCaps(any());
      return this;
    }

    Fixture shouldNotProbeForMatches() {
      pruned();
      verify(node, never()).fieldCaps(any());
      return this;
    }

    /** Runs the pruner once, on the first assertion, so stubbing reads before acting. */
    private IndexName pruned() {
      if (result == null) {
        result =
            timeField == null
                ? new IndexPruner(node).prune(original, filter)
                : new IndexPruner(node).prune(original, filter, timeField);
      }
      return result;
    }
  }
}
