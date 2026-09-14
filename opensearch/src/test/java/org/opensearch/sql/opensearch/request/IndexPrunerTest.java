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
import static org.mockito.Mockito.atLeastOnce;
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
import java.util.Map;
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
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.executor.TimeBounds;
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
      verify(node, atLeastOnce()).fieldCaps(captor.capture());

      FieldCapabilitiesRequest probe = matchingProbe(captor);
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
      verify(node, atLeastOnce()).fieldCaps(captor.capture());

      assertArrayEquals(new String[] {"logs-a-*", "logs-b-*"}, matchingProbe(captor).indices());
    }
  }

  @Nested
  class BoundsDrivenPruning {

    @Test
    void shouldPruneOnTheBoundsOwnRangeWithNoPushedDownFilter() {
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
    }

    /** The picker's field is the index pattern's, which is not always {@code @timestamp}. */
    @Test
    void shouldProbeTheBoundsOwnTimeField() {
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      givenIndexExpression(indices("logs-*", 3), bounds("event_time"))
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
      verify(node, atLeastOnce()).fieldCaps(captor.capture());

      assertArrayEquals(new String[] {"event_time"}, matchingProbe(captor).fields());
    }

    /** As sent: the index's own date parser reads them, so date math survives to the probe. */
    @Test
    void shouldProbeWithTheBoundsExactlyAsSent() {
      ArgumentCaptor<FieldCapabilitiesRequest> captor =
          ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenMatching("logs-a")
          .shouldPruneTo("logs-a");
      verify(node, atLeastOnce()).fieldCaps(captor.capture());

      RangeQueryBuilder range = (RangeQueryBuilder) matchingProbe(captor).indexFilter();
      assertEquals(
          "@timestamp|now-15m|now|true|true",
          String.join(
              "|",
              range.fieldName(),
              String.valueOf(range.from()),
              String.valueOf(range.to()),
              String.valueOf(range.includeLower()),
              String.valueOf(range.includeUpper())));
    }

    @Test
    void shouldNotPruneAConcreteExpression() {
      givenIndexExpression("logs-2024", bounds("@timestamp")).shouldNotPrune().shouldNotProbe();
    }

    @Test
    void shouldNotPruneAnAlias() {
      givenIndexExpression(alias("logs-*"), bounds("@timestamp"))
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    @Test
    void shouldNotPruneADataStream() {
      givenIndexExpression(ds("logs-*"), bounds("@timestamp"))
          .shouldNotPrune()
          .shouldNotProbeForMatches();
    }

    /**
     * An index that does not map the field is reported as not-matching, exactly like one whose
     * values fall outside the window -- so pruning on that would drop it and its rows. #5766
     * review.
     */
    /**
     * An index that does not map the field is reported as not-matching, exactly like one outside
     * the range, so it is added back rather than dropped -- while the ones that are genuinely out
     * of range still go. #5766 review.
     */
    @Test
    void shouldRetainAnIndexThatDoesNotMapTheFieldWhileStillPruning() {
      FieldCapabilities dated = mock(FieldCapabilities.class);
      FieldCapabilities unmapped = mock(FieldCapabilities.class);
      when(unmapped.indices()).thenReturn(new String[] {"logs-c"});
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenMappingThenMatching(
              Map.of("@timestamp", Map.of("date", dated, "unmapped", unmapped)), "logs-a")
          .shouldPruneTo("logs-a,logs-c");
    }

    /** Without the index list there is no way to know what to keep, so nothing is pruned. */
    @Test
    void shouldNotPruneWhenTheProbeDoesNotNameTheUnmappedIndices() {
      FieldCapabilities dated = mock(FieldCapabilities.class);
      FieldCapabilities unmapped = mock(FieldCapabilities.class);
      when(unmapped.indices()).thenReturn(null);
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenFieldMappedAs(Map.of("@timestamp", Map.of("date", dated, "unmapped", unmapped)))
          .shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenTheFieldIsNotADate() {
      FieldCapabilities caps = mock(FieldCapabilities.class);
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenFieldMappedAs(Map.of("@timestamp", Map.of("keyword", caps)))
          .shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenNoIndexMapsTheField() {
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenFieldMappedAs(Map.of())
          .shouldNotPrune();
    }

    @Test
    void shouldNotPruneWhenTheProbeFails() {
      givenIndexExpression(indices("logs-*", 3), bounds("@timestamp"))
          .whenMatchProbeFails()
          .shouldNotPrune();
    }
  }

  /** A probe response reporting the time field as a date, which is what lets pruning proceed. */
  private static Map<String, Map<String, FieldCapabilities>> dateFieldCaps() {
    FieldCapabilities caps = mock(FieldCapabilities.class);
    return Map.of(
        "@timestamp", Map.of("date", caps),
        "event_time", Map.of("date", caps),
        "no_such_field", Map.of());
  }

  /**
   * The matching probe, told apart from the unfiltered mapping probe that precedes it by carrying
   * the index filter.
   */
  private static FieldCapabilitiesRequest matchingProbe(
      ArgumentCaptor<FieldCapabilitiesRequest> captor) {
    return captor.getAllValues().stream()
        .filter(r -> r.indexFilter() != null)
        .reduce((first, second) -> second)
        .orElseThrow(() -> new AssertionError("no filtered probe was issued"));
  }

  private static QueryBuilder timeRange() {
    return rangeQuery("@timestamp").gte("now-1d");
  }

  /** Date math, to show the probe carries the bounds as sent rather than re-interpreting them. */
  private static TimeBounds bounds(String field) {
    return new TimeBounds(field, "now-15m", "now");
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
    return new Fixture(expression, filter, null);
  }

  private Fixture givenIndexExpression(String expression, TimeBounds bounds) {
    return new Fixture(expression, null, bounds);
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
    return new Fixture(resolution.expression(), filter, null);
  }

  private Fixture givenIndexExpression(Resolution resolution, TimeBounds bounds) {
    givenIndexExpression(resolution, (QueryBuilder) null);
    return new Fixture(resolution.expression(), null, bounds);
  }

  private void whenResolved() {
    when(resolveFuture.actionGet(any(TimeValue.class))).thenReturn(resolveResponse);
  }

  private final class Fixture {

    private final IndexName original;
    private final QueryBuilder filter;
    private final TimeBounds bounds;
    private IndexName result;

    Fixture(String expression, QueryBuilder filter, TimeBounds bounds) {
      this.original = new IndexName(expression);
      this.filter = filter;
      this.bounds = bounds;
    }

    Fixture whenMatching(String... matching) {
      // Two field-caps calls now: an unfiltered mapping probe that gates on the field's type, then
      // the filtered one that reports which indices can match.
      when(node.fieldCaps(any())).thenReturn(matchFuture);
      when(matchFuture.actionGet(any(TimeValue.class)))
          .thenReturn(new FieldCapabilitiesResponse(matching, dateFieldCaps()));
      return this;
    }

    /** The mapping probe's answer for the field. */
    Fixture whenFieldMappedAs(Map<String, Map<String, FieldCapabilities>> caps) {
      when(node.fieldCaps(any())).thenReturn(matchFuture);
      when(matchFuture.actionGet(any(TimeValue.class)))
          .thenReturn(new FieldCapabilitiesResponse(new String[] {"logs-a"}, caps));
      return this;
    }

    /**
     * Answers both probes in order: the mapping probe with {@code caps}, then the filtered probe
     * with {@code matching}. One call, because two separate stubbings of the same mock replace each
     * other.
     */
    Fixture whenMappingThenMatching(
        Map<String, Map<String, FieldCapabilities>> caps, String... matching) {
      when(node.fieldCaps(any())).thenReturn(matchFuture);
      when(matchFuture.actionGet(any(TimeValue.class)))
          .thenReturn(new FieldCapabilitiesResponse(new String[] {"logs-a"}, caps))
          .thenReturn(new FieldCapabilitiesResponse(matching, dateFieldCaps()));
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
            bounds == null
                ? new IndexPruner(node).prune(original, filter)
                : new IndexPruner(node).prune(original, bounds);
      }
      return result;
    }
  }
}
