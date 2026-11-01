/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Prunes an index expression down to the concrete indices that can match the query's filter, ahead
 * of operations that do no pruning of their own, such as PIT creation.
 */
@Log4j2
@RequiredArgsConstructor
public class IndexPruner {

  /** Bounds each probe. Generous because a fallback can fail the query, not merely slow it. */
  private static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(30);

  private static final Set<String> IMPLICIT_TIME_FIELDS =
      Set.of(IMPLICIT_FIELD_TIMESTAMP, "timestamp", "time");

  /** Both probes are transport actions, so only the node client can issue them. */
  private final NodeClient node;

  /**
   * Returns the index expression to read. When pruning is safe, that is the list of indices which
   * can match the filter. Otherwise, and on any probe failure, it is the expression the query
   * named.
   *
   * @param indexName index expression the query named
   * @param filter filter pushed down to the search, or null when there is none
   * @return expression to read, never null
   */
  public IndexName prune(IndexName indexName, QueryBuilder filter) {
    return doPrune(indexName, filter, IMPLICIT_TIME_FIELDS);
  }

  /**
   * As {@link #prune(IndexName, QueryBuilder)}, but only considers a range on {@code timeField}. A
   * request-level time range names its own field.
   *
   * @param timeField field a range must be on for the expression to be prunable
   * @return expression to read, never null
   */
  public IndexName prune(IndexName indexName, QueryBuilder filter, String timeField) {
    return doPrune(indexName, filter, Set.of(timeField));
  }

  private IndexName doPrune(
      IndexName indexName, QueryBuilder filter, Set<String> candidateTimeFields) {
    try {
      Optional<String> timeField = findTimeRangeField(filter, candidateTimeFields);
      IndexExpression indexExpr = new IndexExpression(indexName, node);
      if (timeField.isEmpty() || !isPrunable(indexExpr)) {
        log.info("Index pruning skipped: {}", indexExpr);
        return indexName;
      }

      String[] candidates = indexExpr.probeMatching(filter, timeField.get()).getIndices();
      if (candidates.length > 0) {
        return new IndexName(String.join(",", candidates));
      }
      log.info("Index pruning declined: field caps returned no candidate indices");
    } catch (Exception e) {
      log.warn("Index pruning failed; querying the full index expression", e);
    }
    return indexName;
  }

  private static boolean isPrunable(IndexExpression expression) {
    return (expression.hasWildcard() || expression.hasDataStream())
        // An alias may carry a filter or routing that substituting concrete indices would drop.
        && !expression.hasAlias();
  }

  static Optional<String> findTimeRangeField(QueryBuilder query, Set<String> candidateTimeFields) {
    if (query instanceof RangeQueryBuilder range) {
      String field = range.fieldName();
      return candidateTimeFields.contains(field) ? Optional.of(field) : Optional.empty();
    }
    if (query instanceof BoolQueryBuilder bool) {
      return Stream.of(bool.must(), bool.filter(), bool.should())
          .flatMap(List::stream)
          .flatMap(clause -> findTimeRangeField(clause, candidateTimeFields).stream())
          .findFirst();
    }
    if (query instanceof ConstantScoreQueryBuilder constantScore) {
      return findTimeRangeField(constantScore.innerQuery(), candidateTimeFields);
    }
    return Optional.empty();
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
