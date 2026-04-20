/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.FilterType;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder.ScriptQueryUnSupportedException;
import org.opensearch.sql.opensearch.storage.serde.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * Query builder for vector search that keeps the knn query in a scoring (must) context and puts
 * WHERE filters in a non-scoring (filter) context. This prevents the knn relevance scores from
 * being destroyed when a WHERE clause is pushed down.
 *
 * <p>Supports two filter placement strategies via {@link FilterType}:
 *
 * <ul>
 *   <li>{@code POST} — WHERE in {@code bool.filter} outside knn (post-filtering, default)
 *   <li>{@code EFFICIENT} — WHERE inside {@code knn.filter} for pre-filtering during ANN search
 * </ul>
 */
public class VectorSearchQueryBuilder extends OpenSearchIndexScanQueryBuilder {

  private final QueryBuilder knnQuery;
  private final Map<String, String> options;
  private final FilterType filterType;
  private final boolean filterTypeExplicit;
  private final Function<QueryBuilder, QueryBuilder> rebuildKnnWithFilter;
  private boolean filterPushed = false;
  private boolean limitPushed = false;

  /** Full constructor with filter type support. */
  public VectorSearchQueryBuilder(
      OpenSearchRequestBuilder requestBuilder,
      QueryBuilder knnQuery,
      Map<String, String> options,
      FilterType filterType,
      boolean filterTypeExplicit,
      Function<QueryBuilder, QueryBuilder> rebuildKnnWithFilter) {
    super(requestBuilder);
    requestBuilder.getSourceBuilder().query(knnQuery);
    this.knnQuery = knnQuery;
    this.options = options;
    this.filterType = filterType != null ? filterType : FilterType.POST;
    this.filterTypeExplicit = filterTypeExplicit;
    if (this.filterType == FilterType.EFFICIENT && rebuildKnnWithFilter == null) {
      throw new IllegalArgumentException(
          "EFFICIENT filter mode requires a non-null rebuildKnnWithFilter callback");
    }
    this.rebuildKnnWithFilter = rebuildKnnWithFilter;
  }

  /** Default constructor — preserves existing call sites; defaults to POST, not explicit. */
  public VectorSearchQueryBuilder(
      OpenSearchRequestBuilder requestBuilder, QueryBuilder knnQuery, Map<String, String> options) {
    this(requestBuilder, knnQuery, options, FilterType.POST, false, null);
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
    Expression queryCondition = filter.getCondition();

    // Reject WHERE predicates that reference the synthetic _score column. The planner surfaces
    // _score as a projectable/filterable field, but it is not a stored document field in
    // OpenSearch. If we let this pass through, FilterQueryBuilder produces a range query on a
    // non-existent field and the cluster silently returns 0 rows. Users who want a score floor
    // should use option='min_score=...' instead.
    if (containsScoreReference(queryCondition)) {
      throw new ExpressionEvaluationException(
          "WHERE on _score is not supported on vectorSearch()."
              + " Use option='min_score=...' for score-floor filtering.");
    }

    QueryBuilder whereQuery;
    try {
      whereQuery = queryBuilder.build(queryCondition);
    } catch (ScriptQueryUnSupportedException e) {
      if (filterTypeExplicit) {
        throw new ExpressionEvaluationException(
            "filter_type only works when the WHERE clause can be translated to an"
                + " OpenSearch filter. Rewrite the WHERE clause or omit filter_type.");
      }
      // Default mode: fall back to in-memory filtering (matches base class behavior)
      return false;
    }
    filterPushed = true;

    if (filterType == FilterType.EFFICIENT) {
      // knn.filter on AOSS/serverless vector collections rejects script queries. If any WHERE
      // subtree compiled to a ScriptQueryBuilder (arithmetic, function calls, CASE, date math),
      // refuse to embed it under knn.filter instead of shipping a request that will fail at the
      // cluster with an opaque error.
      if (containsScriptQuery(whereQuery)) {
        throw new ExpressionEvaluationException(
            "filter_type=efficient does not support predicates that compile to script queries"
                + " (arithmetic, function calls, CASE, date math). Rewrite the WHERE clause to"
                + " use comparable/term/range predicates, or omit filter_type.");
      }
      QueryBuilder rebuiltKnn = rebuildKnnWithFilter.apply(whereQuery);
      requestBuilder.getSourceBuilder().query(rebuiltKnn);
    } else {
      // POST mode: knn in must (scores), WHERE in filter (no scoring impact)
      BoolQueryBuilder combined = QueryBuilders.boolQuery().must(knnQuery).filter(whereQuery);
      requestBuilder.getSourceBuilder().query(combined);
    }
    return true;
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    // OFFSET on vectorSearch() silently rewrites the search window and drops top results, which
    // defeats the entire point of a relevance-ranked top-k query. The parent path would push
    // `from: <offset>` into the OpenSearch request; reject it explicitly so users get a clear
    // error instead of surprising result shifts.
    if (limit.getOffset() != null && limit.getOffset() != 0) {
      throw new ExpressionEvaluationException(
          "OFFSET is not supported on vectorSearch(). Remove OFFSET and use LIMIT only.");
    }
    validateLimitWithinK(limit.getLimit());
    limitPushed = true;
    return super.pushDownLimit(limit);
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    // Vector search returns results sorted by _score DESC by default.
    // Only _score DESC is meaningful; reject all other sort expressions.
    for (Pair<SortOption, Expression> sortItem : sort.getSortList()) {
      Expression expr = sortItem.getRight();
      if (!(expr instanceof ReferenceExpression)
          || !"_score".equals(((ReferenceExpression) expr).getAttr())) {
        throw new ExpressionEvaluationException(
            String.format(
                "vectorSearch only supports ORDER BY _score DESC; "
                    + "unsupported sort expression: %s",
                expr));
      }
      if (sortItem.getLeft().getSortOrder() != Sort.SortOrder.DESC) {
        throw new ExpressionEvaluationException(
            "vectorSearch only supports ORDER BY _score DESC; _score ASC is not supported");
      }
    }
    // _score DESC is the natural knn order — no need to push the sort itself to OpenSearch.
    // Preserve the parent's sort.getCount() → limit pushdown contract: SQL always sets count=0,
    // but PPL or future callers may set a non-zero count to combine sort+limit in one node.
    if (sort.getCount() != 0) {
      validateLimitWithinK(sort.getCount());
      limitPushed = true;
      requestBuilder.pushDownLimit(sort.getCount(), 0);
    }
    return true;
  }

  /** Validates that the requested limit does not exceed k in top-k mode. */
  private void validateLimitWithinK(int limit) {
    if (options.containsKey("k")) {
      int k = Integer.parseInt(options.get("k"));
      if (limit > k) {
        throw new ExpressionEvaluationException(
            String.format("LIMIT %d exceeds k=%d in top-k vector search", limit, k));
      }
    }
  }

  /**
   * Returns true if any subexpression is a ReferenceExpression whose attr is "_score". Uses the
   * standard ExpressionNodeVisitor so compound predicates (AND/OR/NOT, function calls, CASE) are
   * walked uniformly.
   */
  private static boolean containsScoreReference(Expression expr) {
    AtomicBoolean found = new AtomicBoolean(false);
    expr.accept(
        new ExpressionNodeVisitor<Void, Void>() {
          @Override
          public Void visitReference(ReferenceExpression node, Void context) {
            // Case-insensitive match so _SCORE, _Score, and any quoted/backticked variant that
            // preserves original casing cannot bypass the guard and reach the cluster as a range
            // query on a non-existent field.
            if (node.getAttr() != null && "_score".equalsIgnoreCase(node.getAttr())) {
              found.set(true);
            }
            return null;
          }
        },
        null);
    return found.get();
  }

  /**
   * Recursively scans a QueryBuilder tree for any ScriptQueryBuilder. Handles the common wrappers
   * that FilterQueryBuilder produces: BoolQueryBuilder (must/should/mustNot/filter) and
   * ConstantScoreQueryBuilder. Other QueryBuilder subtypes are leaves for our purposes: if the
   * top-level builder itself is a ScriptQueryBuilder we catch it, otherwise we treat it as
   * script-free.
   */
  private static boolean containsScriptQuery(QueryBuilder qb) {
    if (qb == null) {
      return false;
    }
    if (qb instanceof ScriptQueryBuilder) {
      return true;
    }
    if (qb instanceof BoolQueryBuilder) {
      BoolQueryBuilder bool = (BoolQueryBuilder) qb;
      for (QueryBuilder child : bool.must()) {
        if (containsScriptQuery(child)) {
          return true;
        }
      }
      for (QueryBuilder child : bool.filter()) {
        if (containsScriptQuery(child)) {
          return true;
        }
      }
      for (QueryBuilder child : bool.should()) {
        if (containsScriptQuery(child)) {
          return true;
        }
      }
      for (QueryBuilder child : bool.mustNot()) {
        if (containsScriptQuery(child)) {
          return true;
        }
      }
      return false;
    }
    if (qb instanceof ConstantScoreQueryBuilder) {
      return containsScriptQuery(((ConstantScoreQueryBuilder) qb).innerQuery());
    }
    return false;
  }

  @Override
  public OpenSearchRequestBuilder build() {
    if (filterTypeExplicit && !filterPushed) {
      throw new ExpressionEvaluationException("filter_type requires a pushdownable WHERE clause");
    }
    boolean isRadial = !options.containsKey("k");
    if (isRadial && !limitPushed) {
      throw new ExpressionEvaluationException(
          "LIMIT is required for radial vector search (max_distance or min_score)."
              + " Without LIMIT, the result set size is unbounded.");
    }
    return super.build();
  }
}
