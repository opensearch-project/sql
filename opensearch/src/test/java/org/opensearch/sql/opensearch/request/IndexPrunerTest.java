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
import static org.mockito.ArgumentMatchers.eq;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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

  @Nested
  class FilterShapes {

    @ParameterizedTest(name = "{0}")
    @MethodSource("filters")
    void shouldFindTimeRangeFieldOnlyWhereTheProbeCanUseIt(
        String shape, QueryBuilder filter, String expected) {
      assertEquals(
          Optional.ofNullable(expected),
          IndexPruner.findTimeRangeField(
              filter, Set.of(IMPLICIT_FIELD_TIMESTAMP, "timestamp", "time")));
    }

    private static Stream<Arguments> filters() {
      return Stream.of(
          arguments("bare range on the timestamp", timeRange(), "@timestamp"),
          arguments("range on timestamp", timeRange("timestamp"), "timestamp"),
          arguments("range on time", timeRange("time"), "time"),
          arguments("range under must", boolQuery().must(timeRange()), "@timestamp"),
          arguments("range under filter", boolQuery().filter(timeRange()), "@timestamp"),
          arguments("range under should", boolQuery().should(timeRange()), "@timestamp"),
          arguments("range under constant_score", constantScoreQuery(timeRange()), "@timestamp"),
          arguments("no filter at all", null, null),
          arguments("filter carrying no range", queryStringQuery("error"), null),
          arguments("range on another field", rangeQuery("status").gte(200), null),
          arguments(
              "range under should beside another clause",
              boolQuery().should(timeRange()).should(rangeQuery("status").gte(200)),
              "@timestamp"),
          // A negated clause cannot prove a shard disjoint, so it must not count.
          arguments("range under must_not", boolQuery().mustNot(timeRange()), null));
    }
  }

  @Nested
  class GateRejections {

    @Test
    void shouldNotPruneWhenExpressionIsOneConcreteIndex() {
      givenIndexExpression(indices("logs-2024"), timeRange())
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneWhenFilterHasNoTimestampRange() {
      givenIndexExpression("logs-*", queryStringQuery("error")).shouldNotPrune().shouldNotProbe();
    }
  }

  @Nested
  class PruningDecisions {

    @Test
    void shouldPruneToTheMatchingIndices() {
      givenIndexExpression(indices("logs-*"), timeRange())
          .whenMatching("logs-e", "logs-f")
          .shouldPruneTo("logs-e,logs-f");
    }

    @Test
    void shouldNotPruneWhenNoIndexMatches() {
      givenIndexExpression(indices("logs-*"), timeRange()).whenMatching().shouldNotPrune();
    }

    @Test
    void shouldPreserveRemoteQualifiedNamesFromFieldCaps() {
      givenIndexExpression(ds("remote:logs-*"), timeRange())
          .whenMatching("remote:.ds-logs-a-000003", "remote:.ds-logs-b-000003")
          .shouldPruneTo("remote:.ds-logs-a-000003,remote:.ds-logs-b-000003");
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
    void shouldPruneMultipleDataStreamsByBackingIndex() {
      givenIndexExpression(ds("logs-*"), timeRange())
          .whenMatching(".ds-logs-a-000003", ".ds-logs-b-000003")
          .shouldPruneTo(".ds-logs-a-000003,.ds-logs-b-000003");
    }

    @Test
    void shouldPruneAnExplicitDataStreamNameByBackingIndex() {
      givenIndexExpression(ds("logs-a"), timeRange())
          .whenMatching(".ds-logs-a-000003")
          .shouldPruneTo(".ds-logs-a-000003");
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
      givenIndexExpression(indices("logs-*"), timeRange()).whenMatchProbeFails().shouldNotPrune();
    }
  }

  @Nested
  class ProbeRequest {

    @Test
    void shouldProbeWithTheFilterExpressionAndTimestampField() {
      QueryBuilder filter = timeRange();
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      givenIndexExpression(indices("logs-*"), filter)
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
      verify(node).fieldCaps(captor.capture());

      FieldCapabilitiesRequest probe = captor.getValue();
      assertSame(filter, probe.indexFilter());
      assertArrayEquals(new String[] {"logs-*"}, probe.indices());
      assertEquals(
          "[@timestamp]|" + SearchRequest.DEFAULT_INDICES_OPTIONS,
          Arrays.toString(probe.fields()) + "|" + probe.indicesOptions());
    }

    @Test
    void shouldProbeEveryNameOfACommaSeparatedExpression() {
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      givenIndexExpression(indices("logs-a-*,logs-b-*"), timeRange())
          .whenMatching("logs-a-1")
          .shouldPruneTo("logs-a-1");
      verify(node).fieldCaps(captor.capture());

      assertArrayEquals(new String[] {"logs-a-*", "logs-b-*"}, captor.getValue().indices());
    }
  }

  @Nested
  class BoundsDrivenPruning {

    @Test
    void shouldPruneOnTheBoundsOwnRangeWithNoPushedDownFilter() {
      givenIndexExpression(indices("logs-*"), timeRange("@timestamp"), "@timestamp")
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
    }

    /** The picker's field is the index pattern's, which is not always {@code @timestamp}. */
    @Test
    void shouldProbeTheBoundsOwnTimeField() {
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      givenIndexExpression(indices("logs-*"), timeRange("event_time"), "event_time")
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
      verify(node).fieldCaps(captor.capture());

      assertArrayEquals(new String[] {"event_time"}, captor.getValue().fields());
    }

    /** Forwarded untouched, so date math reaches the index's own parser. */
    @Test
    void shouldProbeWithTheFilterExactlyAsGiven() {
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      QueryBuilder range = timeRange("@timestamp");
      givenIndexExpression(indices("logs-*"), range, "@timestamp")
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
      verify(node).fieldCaps(captor.capture());

      assertSame(range, captor.getValue().indexFilter());
    }

    @Test
    void shouldNotPruneAConcreteExpression() {
      givenIndexExpression(indices("logs-2024"), timeRange("@timestamp"), "@timestamp")
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneAnAlias() {
      givenIndexExpression(alias("logs-*"), timeRange("@timestamp"), "@timestamp")
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldPruneADataStreamOnRequestBounds() {
      givenIndexExpression(ds("logs-*"), timeRange("@timestamp"), "@timestamp")
          .whenMatching(".ds-logs-a-000003", ".ds-logs-b-000003")
          .shouldPruneTo(".ds-logs-a-000003,.ds-logs-b-000003");
    }

    @Test
    void shouldNotPruneWhenTheProbeFails() {
      givenIndexExpression(indices("logs-*"), timeRange("@timestamp"), "@timestamp")
          .whenMatchProbeFails()
          .shouldNotPrune();
    }
  }

  private static QueryBuilder timeRange() {
    return rangeQuery("@timestamp").gte("now-1d");
  }

  /** Date math, to show the probe carries the range as given rather than re-interpreting it. */
  private static QueryBuilder timeRange(String field) {
    return rangeQuery(field).gte("now-15m").lte("now");
  }

  private record Resolution(String expression, Shape shape) {
    private enum Shape {
      INDICES,
      ALIAS,
      DATA_STREAM,
      FAILURE
    }
  }

  private static Resolution indices(String expression) {
    return new Resolution(expression, Resolution.Shape.INDICES);
  }

  private static Resolution alias(String expression) {
    return new Resolution(expression, Resolution.Shape.ALIAS);
  }

  private static Resolution ds(String expression) {
    return new Resolution(expression, Resolution.Shape.DATA_STREAM);
  }

  private static Resolution unresolvable(String expression) {
    return new Resolution(expression, Resolution.Shape.FAILURE);
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
        lenient()
            .when(resolveResponse.getDataStreams())
            .thenReturn(List.of(mock(ResolveIndexAction.ResolvedDataStream.class)));
      }
      case INDICES -> {
        whenResolved();
        when(resolveResponse.getAliases()).thenReturn(List.of());
        lenient().when(resolveResponse.getDataStreams()).thenReturn(List.of());
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
    when(resolveFuture.actionGet(any(TimeValue.class))).thenReturn(resolveResponse);
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
      when(node.fieldCaps(any())).thenReturn(matchFuture);
      when(matchFuture.actionGet(any(TimeValue.class)))
          .thenReturn(new FieldCapabilitiesResponse(matching, Collections.emptyMap()));
      return this;
    }

    Fixture whenMatchProbeFails() {
      when(node.fieldCaps(any())).thenReturn(matchFuture);
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
