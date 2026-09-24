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
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
 * Prunes a wildcard index expression down to the concrete indices that can match the query's
 * filter, ahead of operations that do no pruning of their own, such as PIT creation.
 */
@Log4j2
@RequiredArgsConstructor
public class IndexPruner {

  /** Bounds each probe. Generous because a fallback can fail the query, not merely slow it. */
  private static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(10);

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

      String[] candidates = indexExpr.probeMatching(filter, timeField).getIndices();
      if (0 < candidates.length && indexExpr.isPrunedBy(candidates.length)) {
        // Only now that indices would be dropped: an index whose shards are unavailable cannot be
        // probed, and field caps reports no failure for it -- it is simply absent from the
        // candidates, indistinguishable from an index proven to hold nothing in range. Dropping it
        // would delete its documents from the answer with nothing left to report, because the
        // search that runs then covers every shard it was given: neither the response's shard
        // counts nor allow_partial_search_results would show anything amiss. Pruning may only drop
        // what it proved empty, so keep those and let the search report them as missing shards.
        Set<String> unsearchable = indexExpr.indicesNotProvenEmpty(timeField);
        if (unsearchable.isEmpty()) {
          return new IndexName(String.join(",", candidates));
        }
        Set<String> keep = new LinkedHashSet<>(Arrays.asList(candidates));
        keep.addAll(unsearchable);
        log.info("Index pruning kept unsearchable indices {}", unsearchable);
        if (indexExpr.isPrunedBy(keep.size())) {
          return new IndexName(String.join(",", keep));
        }
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

  private static boolean isPrunable(IndexExpression expression, boolean hasTimeRange) {
    return expression.hasWildcard()
        && hasTimeRange
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

    boolean hasAlias() {
      return !resolved().getAliases().isEmpty();
    }

    boolean hasDataStream() {
      return !resolved().getDataStreams().isEmpty();
    }

    boolean isPrunedBy(int candidateCount) {
      return candidateCount < resolved().getIndices().size();
    }

    FieldCapabilitiesResponse probeMatching(QueryBuilder filter, String timeField) {
      return probe(filter, timeField);
    }

    /**
     * Resolved indices the matching probe cannot have ruled out, so pruning must keep them: those
     * absent from an unfiltered probe. A readable index reports its field caps whatever the time
     * range, so absence there means the index could not be read at all -- which is what the
     * filtered probe cannot distinguish from "holds nothing in range".
     *
     * <p>This holds regardless of what the cluster state believes, so it also covers the window
     * after a node stops but before the cluster manager marks its shards unassigned -- seconds, and
     * precisely when a dashboard refresh would otherwise lose the index silently.
     *
     * <p>Deliberately probe-based rather than reading the routing table: a cluster state request
     * needs {@code cluster:monitor/state}, which an index-scoped role does not carry, so it would
     * both disable pruning for those users and log a missing-privileges audit event on every query.
     * The cost is that an index readable through its other shards while one shard is unassigned
     * still looks prunable; its documents in range would have to live only on the missing shard,
     * which takes custom routing to arrange.
     *
     * <p>An index that is readable but does not map {@code timeField} is also absent from the
     * unfiltered probe, so it is kept too. That costs a shard in the search and no correctness.
     */
    Set<String> indicesNotProvenEmpty(String timeField) {
      // Not Set.of: it rejects a duplicate or null name, which would turn a probe quirk into a
      // lost optimization for the whole query.
      Set<String> readable = new HashSet<>(Arrays.asList(probe(null, timeField).getIndices()));
      Set<String> missing = new LinkedHashSet<>();
      for (ResolveIndexAction.ResolvedIndex index : resolved().getIndices()) {
        if (!readable.contains(index.getName())) {
          missing.add(index.getName());
        }
      }
      return missing;
    }

    private FieldCapabilitiesResponse probe(@Nullable QueryBuilder filter, String timeField) {
      FieldCapabilitiesRequest request =
          new FieldCapabilitiesRequest()
              .indices(indexName.getIndexNames())
              .fields(timeField)
              // Must expand as the search will, or candidates describe a different index set.
              .indicesOptions(DEFAULT_INDICES_OPTIONS);
      if (filter != null) {
        request.indexFilter(filter);
      }
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
