/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.executor.TimeBounds;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Prunes a wildcard index expression down to the concrete indices that can match the query's
 * filter, ahead of operations that do no pruning of their own, such as PIT creation.
 */
@Log4j2
@RequiredArgsConstructor
public class IndexPruner {

  /** Bounds each probe. Generous because a fallback can fail the query, not merely slow it. */
  private static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(10);

  /**
   * Date formats a bound is parsed with. The first two are OpenSearch's own defaults; the third is
   * a UTC wall clock with no zone designator, which is what a client writing the same literal into
   * the query text tends to produce -- PPL accepts it there, so refusing it here would mean bounds
   * that cannot describe the filter beside them. Date math is resolved before any of these apply,
   * so {@code now-7d} is unaffected.
   *
   * <p>This <em>replaces</em> the format the field declares, rather than adding to it. A field with
   * a custom format therefore only prunes on a bound spelled one of these ways; a bound in the
   * field's own exotic format fails to parse, the probe errors, and pruning declines -- costing the
   * optimization but never a row, since declining reads the full expression.
   */
  private static final String BOUND_FORMATS =
      "strict_date_optional_time||epoch_millis||yyyy-MM-dd HH:mm:ss.SSS";

  /** The bucket _field_caps uses for indices that do not map the requested field. */
  private static final String UNMAPPED = "unmapped";

  /** Mapping types a range predicate can prune on. */
  private static final Set<String> DATE_FIELD_TYPES = Set.of("date", "date_nanos");

  /** Both probes are transport actions, so only the node client can issue them. */
  private final NodeClient node;

  /**
   * Returns the index expression to read. When pruning is safe and narrows the read, that is the
   * list of indices which can match the filter. Otherwise, and on any probe failure, it is the
   * expression the query named.
   *
   * @param indexName index expression the query named
   * @param filter filter pushed down to the search, or null when there is none
   * @return expression to read, never null
   */
  public IndexName prune(IndexName indexName, QueryBuilder filter) {
    return prune(indexName, filter, containsTimeRange(filter), IMPLICIT_FIELD_TIMESTAMP, false);
  }

  /**
   * Returns the index expression to read, narrowed to the indices that can hold data in {@code
   * bounds}.
   *
   * <p>Unlike {@link #prune(IndexName, QueryBuilder)} this needs no pushed-down filter to read a
   * range out of, so it can run while the table is still being resolved -- before the expression is
   * expanded and every matched index's mapping merged, which is the cost a wildcard over months of
   * rollovers actually pays.
   *
   * @param indexName index expression the query named
   * @param bounds request-level bounds
   * @return expression to read, never null
   */
  public IndexName prune(IndexName indexName, TimeBounds bounds) {
    // The bounds go to the probe as the strings the request sent, so OpenSearch's own date parser
    // reads them -- date math included. Re-interpreting them here could produce a narrower window
    // than the caller meant and drop an index that can match.
    QueryBuilder range =
        new RangeQueryBuilder(bounds.getTimeField())
            .gte(bounds.getStart())
            .lte(bounds.getEnd())
            .format(BOUND_FORMATS);
    return prune(indexName, range, true, bounds.getTimeField(), true);
  }

  /**
   * @param retainIndicesWithoutTimeField whether an index that does not map {@code timeField} has
   *     to be kept. It does when the range came from a request parameter, since the query text need
   *     not filter on the field at all and that index's rows are still wanted. It does not when the
   *     range was read out of the query's own pushed-down filter: an index that cannot satisfy that
   *     filter returns nothing regardless, so dropping it is lossless.
   */
  private IndexName prune(
      IndexName indexName,
      QueryBuilder filter,
      boolean hasTimeRange,
      String timeField,
      boolean retainIndicesWithoutTimeField) {
    try {
      IndexExpression indexExpr = new IndexExpression(indexName, node);
      if (!isPrunable(indexExpr, hasTimeRange)) {
        log.info("Index pruning skipped: {}", indexExpr);
        return indexName;
      }

      FieldMapping mapping =
          retainIndicesWithoutTimeField ? indexExpr.probeMapping(timeField) : FieldMapping.ANY;
      if (mapping.declineReason() != null) {
        log.info("Index pruning skipped: {}", mapping.declineReason());
        return indexName;
      }

      Set<String> candidates =
          new LinkedHashSet<>(
              Arrays.asList(indexExpr.probeMatching(filter, timeField).getIndices()));
      // An index that does not map the field is reported as not-matching, exactly like one whose
      // values fall outside the range -- so it has to be added back rather than inferred away.
      candidates.addAll(mapping.indicesWithoutField());
      if (!candidates.isEmpty() && indexExpr.isPrunedBy(candidates.size())) {
        return new IndexName(String.join(",", candidates));
      }
      log.info(
          "Index pruning declined: {} of {} indices retained",
          candidates.size(),
          indexExpr.resolved.getIndices().size());
    } catch (Exception e) {
      log.warn("Index pruning failed; querying the full index expression", e);
    }
    return indexName;
  }

  private static boolean isPrunable(IndexExpression expression, boolean hasTimeRange) {
    return expression.hasWildcard()
        && hasTimeRange
        // Keep these last: unlike the gates above, they resolve the expression. An alias may carry
        // a filter of its own, which substituting its concrete indices would silently drop.
        && !expression.hasAlias()
        && !expression.hasDataStream();
  }

  /**
   * What the mapping probe says about the time field: either why a range on it cannot decide which
   * indices to keep, or which indices do not map it at all and so must be retained regardless of
   * the range.
   *
   * <p>The distinction matters because {@code _field_caps} reports an index that does not map the
   * field as not-matching, indistinguishably from one whose values fall outside the range. Pruning
   * on that alone would drop the index and every row in it.
   */
  record FieldMapping(@Nullable String declineReason, Set<String> indicesWithoutField) {

    /** No mapping probe was run, so nothing is required to be kept. */
    static final FieldMapping ANY = new FieldMapping(null, Set.of());

    static FieldMapping decline(String reason) {
      return new FieldMapping(reason, Set.of());
    }
  }

  static FieldMapping readMapping(Map<String, FieldCapabilities> byType, String timeField) {
    if (byType == null || byType.isEmpty()) {
      return FieldMapping.decline(String.format("[%s] is mapped by no queried index", timeField));
    }
    Set<String> withoutField = new LinkedHashSet<>();
    for (Map.Entry<String, FieldCapabilities> entry : byType.entrySet()) {
      if (UNMAPPED.equals(entry.getKey())) {
        String[] indices = entry.getValue().indices();
        if (indices == null) {
          // Only null when the field is mapped uniformly, which contradicts an unmapped bucket
          // existing; treat the unknown set as unprunable rather than guess.
          return FieldMapping.decline(
              String.format("[%s] is unmapped in indices the probe did not name", timeField));
        }
        withoutField.addAll(Arrays.asList(indices));
      } else if (!DATE_FIELD_TYPES.contains(entry.getKey())) {
        return FieldMapping.decline(
            String.format(
                "[%s] is mapped as %s somewhere, which a range cannot prune on",
                timeField, entry.getKey()));
      }
    }
    if (withoutField.size() == byType.size() && !byType.containsKey(UNMAPPED)) {
      return FieldMapping.decline(String.format("[%s] is not a date anywhere", timeField));
    }
    return new FieldMapping(null, withoutField);
  }

  static boolean containsTimeRange(QueryBuilder query) {
    if (query instanceof RangeQueryBuilder range) {
      return IMPLICIT_FIELD_TIMESTAMP.equals(range.fieldName());
    }
    if (query instanceof BoolQueryBuilder bool) {
      return Stream.of(bool.must(), bool.filter(), bool.should())
          .flatMap(List::stream)
          .anyMatch(IndexPruner::containsTimeRange);
    }
    if (query instanceof ConstantScoreQueryBuilder constantScore) {
      return containsTimeRange(constantScore.innerQuery());
    }
    return false;
  }

  /** The index expression a query named, resolving itself the first time it is asked to. */
  static final class IndexExpression {

    private final IndexName indexName;
    private final NodeClient node;
    private ResolveIndexAction.Response resolved;

    IndexExpression(IndexName indexName, NodeClient node) {
      this.indexName = indexName;
      this.node = node;
    }

    boolean hasWildcard() {
      return Arrays.stream(indexName.getIndexNames()).anyMatch(Regex::isSimpleMatchPattern);
    }

    boolean hasAlias() {
      return !resolved().getAliases().isEmpty();
    }

    boolean hasDataStream() {
      return !resolved().getDataStreams().isEmpty();
    }

    boolean isPrunedBy(int candidateCount) {
      return candidateCount < resolved().getIndices().size();
    }

    /**
     * Runs the mapping probe -- unfiltered, since a filter would hide the very indices this has to
     * see -- and reports what it says about the field.
     */
    FieldMapping probeMapping(String timeField) {
      FieldCapabilitiesRequest request =
          new FieldCapabilitiesRequest()
              .indices(indexName.getIndexNames())
              .fields(timeField)
              .includeUnmapped(true)
              .indicesOptions(DEFAULT_INDICES_OPTIONS);
      FieldCapabilitiesResponse probe = node.fieldCaps(request).actionGet(PROBE_TIMEOUT);
      return IndexPruner.readMapping(probe.getField(timeField), timeField);
    }

    FieldCapabilitiesResponse probeMatching(QueryBuilder filter, String timeField) {
      FieldCapabilitiesRequest request =
          new FieldCapabilitiesRequest()
              .indices(indexName.getIndexNames())
              .fields(timeField)
              .indexFilter(filter)
              // Must expand as the search will, or candidates describe a different index set.
              .indicesOptions(DEFAULT_INDICES_OPTIONS);
      return node.fieldCaps(request).actionGet(PROBE_TIMEOUT);
    }

    @Override
    public String toString() {
      return String.format(
          "wildcard=%s, alias=%s, dataStream=%s",
          hasWildcard(),
          // Guarded so neither a log nor a debugger inspection can fire a resolve probe.
          resolved == null ? "n/a" : hasAlias(),
          resolved == null ? "n/a" : hasDataStream());
    }

    private ResolveIndexAction.Response resolved() {
      if (resolved == null) {
        ResolveIndexAction.Request request =
            new ResolveIndexAction.Request(indexName.getIndexNames(), DEFAULT_INDICES_OPTIONS);
        resolved = node.execute(ResolveIndexAction.INSTANCE, request).actionGet(PROBE_TIMEOUT);
      }
      return resolved;
    }
  }
}
