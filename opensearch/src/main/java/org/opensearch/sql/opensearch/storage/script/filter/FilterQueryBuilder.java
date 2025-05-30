/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import static java.util.Collections.emptyMap;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.opensearch.sql.opensearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.core.ExpressionScript;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LikeQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.NestedQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.RangeQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.RangeQuery.Comparison;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.TermQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchBoolPrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhrasePrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MultiMatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryStringQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.SimpleQueryStringQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.WildcardQuery;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@RequiredArgsConstructor
public class FilterQueryBuilder extends ExpressionNodeVisitor<QueryBuilder, Object> {

  public static class ScriptQueryUnSupportedException extends RuntimeException {
    public ScriptQueryUnSupportedException(String message) {
      super(message);
    }
  }

  /** Serializer that serializes expression for build DSL query. */
  private final ExpressionSerializer serializer;

  /** Mapping from function name to lucene query builder. */
  private final Map<FunctionName, LuceneQuery> luceneQueries =
      ImmutableMap.<FunctionName, LuceneQuery>builder()
          .put(BuiltinFunctionName.EQUAL.getName(), new TermQuery())
          .put(BuiltinFunctionName.LESS.getName(), new RangeQuery(Comparison.LT))
          .put(BuiltinFunctionName.GREATER.getName(), new RangeQuery(Comparison.GT))
          .put(BuiltinFunctionName.LTE.getName(), new RangeQuery(Comparison.LTE))
          .put(BuiltinFunctionName.GTE.getName(), new RangeQuery(Comparison.GTE))
          .put(BuiltinFunctionName.LIKE.getName(), new LikeQuery())
          .put(BuiltinFunctionName.MATCH.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MATCH_PHRASE.getName(), new MatchPhraseQuery())
          .put(BuiltinFunctionName.MATCHPHRASE.getName(), new MatchPhraseQuery())
          .put(BuiltinFunctionName.MATCHPHRASEQUERY.getName(), new MatchPhraseQuery())
          .put(BuiltinFunctionName.QUERY.getName(), new QueryQuery())
          .put(BuiltinFunctionName.MATCH_QUERY.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MATCHQUERY.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MULTI_MATCH.getName(), new MultiMatchQuery())
          .put(BuiltinFunctionName.MULTIMATCH.getName(), new MultiMatchQuery())
          .put(BuiltinFunctionName.MULTIMATCHQUERY.getName(), new MultiMatchQuery())
          .put(BuiltinFunctionName.SIMPLE_QUERY_STRING.getName(), new SimpleQueryStringQuery())
          .put(BuiltinFunctionName.QUERY_STRING.getName(), new QueryStringQuery())
          .put(BuiltinFunctionName.MATCH_BOOL_PREFIX.getName(), new MatchBoolPrefixQuery())
          .put(BuiltinFunctionName.MATCH_PHRASE_PREFIX.getName(), new MatchPhrasePrefixQuery())
          .put(BuiltinFunctionName.WILDCARD_QUERY.getName(), new WildcardQuery())
          .put(BuiltinFunctionName.WILDCARDQUERY.getName(), new WildcardQuery())
          .put(BuiltinFunctionName.NESTED.getName(), new NestedQuery())
          .build();

  /**
   * Build OpenSearch filter query from expression.
   *
   * @param expr expression
   * @return query
   */
  public QueryBuilder build(Expression expr) {
    return expr.accept(this, null);
  }

  @Override
  public QueryBuilder visitFunction(FunctionExpression func, Object context) {
    FunctionName name = func.getFunctionName();
    switch (name.getFunctionName()) {
      case "and":
        return buildBoolQuery(func, context, BoolQueryBuilder::filter);
      case "or":
        return buildBoolQuery(func, context, BoolQueryBuilder::should);
      case "not":
        return buildBoolQuery(func, context, BoolQueryBuilder::mustNot);
      case "nested":
        // TODO Fill in case when adding support for syntax - nested(path, condition)
        throw new SyntaxCheckException(
            "Invalid syntax used for nested function in WHERE clause: "
                + "nested(field | field, path) OPERATOR LITERAL");
      default:
        {
          LuceneQuery query = luceneQueries.get(name);
          if (query != null && query.canSupport(func)) {
            return query.build(func);
          } else if (query != null && query.isNestedPredicate(func)) {
            NestedQuery nestedQuery =
                (NestedQuery)
                    luceneQueries.get(
                        ((FunctionExpression) func.getArguments().get(0)).getFunctionName());
            return nestedQuery.buildNested(func, query);
          }
          return buildScriptQuery(func);
        }
    }
  }

  private BoolQueryBuilder buildBoolQuery(
      FunctionExpression node,
      Object context,
      BiFunction<BoolQueryBuilder, QueryBuilder, QueryBuilder> accumulator) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    for (Expression arg : node.getArguments()) {
      accumulator.apply(boolQuery, arg.accept(this, context));
    }
    return boolQuery;
  }

  private ScriptQueryBuilder buildScriptQuery(FunctionExpression node) {
    Set<ReferenceExpression> fields = ExpressionScript.extractFields(node);
    if (fields.stream().anyMatch(field -> field.getType() == ExprCoreType.STRUCT)) {
      throw new ScriptQueryUnSupportedException(
          "Script query does not support fields of struct type in OpenSearch.");
    }
    return new ScriptQueryBuilder(
        new Script(
            DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(node), emptyMap()));
  }
}
