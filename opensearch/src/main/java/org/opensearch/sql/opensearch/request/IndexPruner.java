/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.RemoteClusterAware;
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

  private static final int MAX_LOGGED_NAMES = 5;

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
    return prune(indexName, filter, IMPLICIT_FIELD_TIMESTAMP);
  }

  /**
   * As {@link #prune(IndexName, QueryBuilder)}, but ranged on {@code timeField} rather than {@code
   * @timestamp}. A request-level time range names its own field.
   *
   * @param timeField field a range must be on for the expression to be prunable
   * @return expression to read, never null
   */
  public IndexName prune(IndexName indexName, QueryBuilder filter, String timeField) {
    try {
      IndexExpression indexExpr = new IndexExpression(indexName, node);
      if (!isPrunable(indexExpr, containsTimeRange(filter, timeField))) {
        log.info("Index pruning skipped: {}", indexExpr);
        return indexName;
      }

      ActionFuture<FieldCapabilitiesResponse> matching = indexExpr.probe(filter, timeField);
      // Sent now so it overlaps the match probe; read only if pruning narrows.
      ActionFuture<FieldCapabilitiesResponse> readable = indexExpr.probe(null, timeField);
      String[] candidates = matching.actionGet(PROBE_TIMEOUT).getIndices();
      if (0 < candidates.length && indexExpr.isPrunedBy(candidates.length)) {
        // An unreadable index is missing from the match probe just as an empty one is; keep it.
        Set<String> unreadable = indexExpr.unreadableIndices(readable.actionGet(PROBE_TIMEOUT));
        Set<String> keep = new LinkedHashSet<>(Arrays.asList(candidates));
        keep.addAll(unreadable);
        if (!unreadable.isEmpty()) {
          log.info(
              "Index pruning kept {} unreadable indices: {}", unreadable.size(), cap(unreadable));
        }
        if (indexExpr.isPrunedBy(keep.size())) {
          return new IndexName(String.join(",", keep));
        }
        log.info("Index pruning declined: keeping unreadable indices left nothing to drop");
        return indexName;
      }
      log.info(
          "Index pruning declined: {} of {} indices matched",
          candidates.length,
          indexExpr.resolved.getIndices().size());
    } catch (Exception e) {
      log.warn("Index pruning failed; querying the full index expression", e);
    }
    return indexName;
  }

  private static String cap(Set<String> names) {
    String spelled =
        names.stream().limit(MAX_LOGGED_NAMES).collect(Collectors.joining(", ", "[", "]"));
    int remaining = names.size() - MAX_LOGGED_NAMES;
    return remaining > 0 ? spelled + " and " + remaining + " more" : spelled;
  }

  private static boolean isPrunable(IndexExpression expression, boolean hasTimeRange) {
    return expression.hasWildcard()
        && hasTimeRange
        // Field caps and resolve omit an unreachable remote cluster without error.
        && !expression.hasRemoteCluster()
        // Last: these resolve the expression. An alias may carry a filter that substituting its
        // concrete indices would drop.
        && !expression.hasAlias()
        && !expression.hasDataStream();
  }

  static boolean containsTimeRange(QueryBuilder query, String timeField) {
    if (query instanceof RangeQueryBuilder range) {
      return timeField.equals(range.fieldName());
    }
    if (query instanceof BoolQueryBuilder bool) {
      return Stream.of(bool.must(), bool.filter(), bool.should())
          .flatMap(List::stream)
          .anyMatch(clause -> containsTimeRange(clause, timeField));
    }
    if (query instanceof ConstantScoreQueryBuilder constantScore) {
      return containsTimeRange(constantScore.innerQuery(), timeField);
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

    boolean hasRemoteCluster() {
      return Arrays.stream(indexName.getIndexNames())
          .anyMatch(name -> name.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
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

    /** Resolved indices the unfiltered probe could not read, judged without cluster state. */
    Set<String> unreadableIndices(FieldCapabilitiesResponse readableProbe) {
      Set<String> unreadable = names(resolved());
      unreadable.removeAll(new HashSet<>(Arrays.asList(readableProbe.getIndices())));
      if (!unreadable.isEmpty()) {
        // Re-resolve: naming an index deleted since would fail the search.
        unreadable.retainAll(names(resolve()));
      }
      return unreadable;
    }

    ActionFuture<FieldCapabilitiesResponse> probe(@Nullable QueryBuilder filter, String timeField) {
      FieldCapabilitiesRequest request =
          new FieldCapabilitiesRequest()
              .indices(indexName.getIndexNames())
              .fields(timeField)
              // Must expand as the search will, or candidates describe a different index set.
              .indicesOptions(DEFAULT_INDICES_OPTIONS);
      if (filter != null) {
        request.indexFilter(filter);
      }
      return node.fieldCaps(request);
    }

    @Override
    public String toString() {
      return String.format(
          "wildcard=%s, remote=%s, alias=%s, dataStream=%s",
          hasWildcard(),
          hasRemoteCluster(),
          // Guarded so neither a log nor a debugger inspection can fire a resolve probe.
          resolved == null ? "n/a" : hasAlias(),
          resolved == null ? "n/a" : hasDataStream());
    }

    private ResolveIndexAction.Response resolved() {
      if (resolved == null) {
        resolved = resolve();
      }
      return resolved;
    }

    private ResolveIndexAction.Response resolve() {
      ResolveIndexAction.Request request =
          new ResolveIndexAction.Request(indexName.getIndexNames(), DEFAULT_INDICES_OPTIONS);
      return node.execute(ResolveIndexAction.INSTANCE, request).actionGet(PROBE_TIMEOUT);
    }

    private static Set<String> names(ResolveIndexAction.Response response) {
      return response.getIndices().stream()
          .map(ResolveIndexAction.ResolvedIndex::getName)
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }
  }
}
