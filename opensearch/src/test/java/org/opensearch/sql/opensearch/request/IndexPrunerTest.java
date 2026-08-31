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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    void shouldSeeATimestampRangeOnlyWhereTheProbeCanUseIt(
        String shape, QueryBuilder filter, boolean expected) {
      assertEquals(expected, IndexPruner.containsTimeRange(filter));
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
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      givenIndexExpression(indices("logs-*", 3), filter)
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
      givenIndexExpression(indices("logs-a-*,logs-b-*", 3), timeRange())
          .whenMatching("logs-a-1")
          .shouldPruneTo("logs-a-1");
      verify(node).fieldCaps(captor.capture());

      assertArrayEquals(new String[] {"logs-a-*", "logs-b-*"}, captor.getValue().indices());
    }
  }

  private static QueryBuilder timeRange() {
    return rangeQuery("@timestamp").gte("now-1d");
  }

  /** What the resolve probe reports for an expression. */
  private record Resolution(String expression, int indexCount, Shape shape) {
    private enum Shape {
      INDICES,
      ALIAS,
      DATA_STREAM,
      FAILURE
    }
  }

  private static Resolution indices(String expression, int indexCount) {
    return new Resolution(expression, indexCount, Resolution.Shape.INDICES);
  }

  private static Resolution alias(String expression) {
    return new Resolution(expression, 0, Resolution.Shape.ALIAS);
  }

  private static Resolution ds(String expression) {
    return new Resolution(expression, 0, Resolution.Shape.DATA_STREAM);
  }

  private static Resolution unresolvable(String expression) {
    return new Resolution(expression, 0, Resolution.Shape.FAILURE);
  }

  /** An expression the gates reject, so nothing is ever resolved. */
  private Fixture givenIndexExpression(String expression, QueryBuilder filter) {
    return new Fixture(expression, filter);
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
        // Lenient because the count is read only once a match list exists, so a test whose probe
        // throws or matches nothing never consumes it.
        lenient()
            .when(resolveResponse.getIndices())
            .thenReturn(
                Collections.nCopies(
                    resolution.indexCount(), mock(ResolveIndexAction.ResolvedIndex.class)));
      }
    }
    return new Fixture(resolution.expression(), filter);
  }

  private void whenResolved() {
    when(resolveFuture.actionGet(any(TimeValue.class))).thenReturn(resolveResponse);
  }

  private final class Fixture {

    private final IndexName original;
    private final QueryBuilder filter;
    private IndexName result;

    Fixture(String expression, QueryBuilder filter) {
      this.original = new IndexName(expression);
      this.filter = filter;
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
        result = new IndexPruner(node).prune(original, filter);
      }
      return result;
    }
  }
}
