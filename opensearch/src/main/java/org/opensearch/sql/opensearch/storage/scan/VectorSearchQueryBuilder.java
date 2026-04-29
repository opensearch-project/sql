/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
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
 * Query builder for vector search. The knn relevance score is preserved regardless of placement
 * strategy — in {@code EFFICIENT} mode the knn query carries its own scores, and in {@code POST}
 * mode the knn query sits in a scoring ({@code must}) context while the WHERE clause is applied as
 * a non-scoring ({@code filter}) clause.
 *
 * <p>Supports two filter placement strategies via {@link FilterType}:
 *
 * <ul>
 *   <li>{@code EFFICIENT} — WHERE inside {@code knn.filter} for pre-filtering during ANN search
 *       (default).
 *   <li>{@code POST} — WHERE in {@code bool.filter} outside knn (post-filtering fallback, used when
 *       the WHERE shape is not compatible with pre-filtering).
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
    this.filterType = filterType != null ? filterType : FilterType.EFFICIENT;
    this.filterTypeExplicit = filterTypeExplicit;
    if (this.filterType == FilterType.EFFICIENT && rebuildKnnWithFilter == null) {
      throw new IllegalArgumentException(
          "EFFICIENT filter mode requires a non-null rebuildKnnWithFilter callback");
    }
    this.rebuildKnnWithFilter = rebuildKnnWithFilter;
  }

  /**
   * Test-only constructor — pins {@link FilterType#POST} so callers that do not wire a {@code
   * rebuildKnnWithFilter} callback (unit tests) can still exercise the push-down contract.
   * Production callers always go through the full constructor, which defaults to {@link
   * FilterType#EFFICIENT}.
   */
  public VectorSearchQueryBuilder(
      OpenSearchRequestBuilder requestBuilder, QueryBuilder knnQuery, Map<String, String> options) {
    this(requestBuilder, knnQuery, options, FilterType.POST, false, null);
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
    Expression queryCondition = filter.getCondition();

    // _score is synthetic, not a stored field; a range query on it silently returns 0 rows.
    // Users who want a score floor should use option='min_score=...'.
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
      // Fail closed: knn.filter on AOSS rejects script queries and nested predicates expand the
      // preview contract. Allow-list validator beats a blacklist walker.
      validateEfficientFilterSafe(whereQuery);
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
    // OFFSET would shift the search window and silently drop top results; reject with a clear
    // error rather than have the parent path push `from: <offset>` into the request.
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
    // _score DESC is knn's natural order, so the sort itself is not pushed. Preserve the
    // parent's sort.getCount() → limit contract; SQL sends 0, PPL may combine sort+limit.
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

  // True if any ReferenceExpression in the tree names _score (case-insensitive, so quoted/
  // backticked variants cannot bypass the guard).
  private static boolean containsScoreReference(Expression expr) {
    AtomicBoolean found = new AtomicBoolean(false);
    expr.accept(
        new ExpressionNodeVisitor<Void, Void>() {
          @Override
          public Void visitReference(ReferenceExpression node, Void context) {
            if (node.getAttr() != null && "_score".equalsIgnoreCase(node.getAttr())) {
              found.set(true);
            }
            return null;
          }
        },
        null);
    return found.get();
  }

  // Allow-list of leaf query types FilterQueryBuilder emits today. Any new wrapper or container
  // appearing here must fail closed rather than silently embed under knn.filter.
  private static final Set<Class<? extends QueryBuilder>> SAFE_EFFICIENT_FILTER_LEAVES =
      Set.of(
          TermQueryBuilder.class,
          RangeQueryBuilder.class,
          WildcardQueryBuilder.class,
          MatchQueryBuilder.class,
          MatchPhraseQueryBuilder.class,
          MatchPhrasePrefixQueryBuilder.class,
          MultiMatchQueryBuilder.class,
          QueryStringQueryBuilder.class,
          SimpleQueryStringBuilder.class,
          MatchBoolPrefixQueryBuilder.class,
          ExistsQueryBuilder.class);

  // Package-private for direct branch coverage in unit tests. Fail-closed: recurse known
  // containers, reject ScriptQueryBuilder/NestedQueryBuilder with targeted messages, allow
  // listed leaves, reject everything else as unsupported shape.
  static void validateEfficientFilterSafe(QueryBuilder qb) {
    if (qb == null) {
      return;
    }
    if (qb instanceof ScriptQueryBuilder) {
      throw new ExpressionEvaluationException(
          "vectorSearch WHERE pre-filtering does not support predicates that compile to"
              + " script queries (arithmetic, function calls, CASE, date math). Rewrite the"
              + " WHERE clause to use term/range/bool predicates, or set filter_type=post to"
              + " apply the predicate after the k-NN search.");
    }
    if (qb instanceof BoolQueryBuilder) {
      BoolQueryBuilder bool = (BoolQueryBuilder) qb;
      bool.must().forEach(VectorSearchQueryBuilder::validateEfficientFilterSafe);
      bool.filter().forEach(VectorSearchQueryBuilder::validateEfficientFilterSafe);
      bool.should().forEach(VectorSearchQueryBuilder::validateEfficientFilterSafe);
      bool.mustNot().forEach(VectorSearchQueryBuilder::validateEfficientFilterSafe);
      return;
    }
    if (qb instanceof ConstantScoreQueryBuilder) {
      validateEfficientFilterSafe(((ConstantScoreQueryBuilder) qb).innerQuery());
      return;
    }
    if (qb instanceof NestedQueryBuilder) {
      throw new ExpressionEvaluationException(
          "vectorSearch WHERE pre-filtering does not support nested predicates in this"
              + " preview. Rewrite the WHERE clause using non-nested fields, or set"
              + " filter_type=post to apply the predicate after the k-NN search.");
    }
    if (SAFE_EFFICIENT_FILTER_LEAVES.contains(qb.getClass())) {
      return;
    }
    throw new ExpressionEvaluationException(
        "vectorSearch WHERE pre-filtering encountered an unsupported filter query shape: "
            + qb.getClass().getSimpleName()
            + ". Rewrite the WHERE clause using simple term/range/bool predicates, or set"
            + " filter_type=post to apply the predicate after the k-NN search.");
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
