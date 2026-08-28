/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;
import static org.opensearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
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

  /** Caps the substituted expression, not what is read: the wildcard fallback reads no fewer. */
  private static final int MAX_PRUNED_INDICES = 50;

  /** Bounds each probe. The match probe can cost a round trip per shard. */
  private static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(5);

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
    try {
      IndexExpression indexExpr = new IndexExpression(indexName, node);
      if (!isPrunable(indexExpr, filter)) {
        log.info("Index pruning skipped: {}", indexExpr);
        return indexName;
      }

      String[] candidates = indexExpr.probeMatching(filter);
      if (0 < candidates.length
          && candidates.length <= MAX_PRUNED_INDICES
          && indexExpr.isPrunedBy(candidates)) {
        return new IndexName(String.join(",", candidates));
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

  private static boolean isPrunable(IndexExpression expression, QueryBuilder filter) {
    return expression.hasWildcard()
        && !expression.isCrossCluster()
        && containsTimeRange(filter)
        // Keep these last: unlike the gates above, they resolve the expression.
        && !expression.hasAlias()
        && !expression.hasDataStream();
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

    boolean isCrossCluster() {
      return Arrays.stream(indexName.getIndexNames())
          .anyMatch(name -> name.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
    }

    boolean hasAlias() {
      return !resolved().getAliases().isEmpty();
    }

    boolean hasDataStream() {
      return !resolved().getDataStreams().isEmpty();
    }

    boolean isPrunedBy(String[] candidates) {
      return candidates.length < resolved().getIndices().size();
    }

    String[] probeMatching(QueryBuilder filter) {
      FieldCapabilitiesRequest request =
          new FieldCapabilitiesRequest()
              .indices(indexName.getIndexNames())
              .fields(IMPLICIT_FIELD_TIMESTAMP)
              .indexFilter(filter)
              // Must expand as the search will, or candidates describe a different index set.
              .indicesOptions(DEFAULT_INDICES_OPTIONS);
      return node.fieldCaps(request).actionGet(PROBE_TIMEOUT).getIndices();
    }

    @Override
    public String toString() {
      return String.format(
          "wildcard=%s, crossCluster=%s, alias=%s, dataStream=%s",
          hasWildcard(),
          isCrossCluster(),
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
