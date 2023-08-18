/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.nestedfield;

import static com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.existsQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;
import org.opensearch.sql.legacy.utils.StringUtils;

/** Nested field projection class to make OpenSearch return matched rows in nested field. */
public class NestedFieldProjection {

  private final SearchRequestBuilder request;

  public NestedFieldProjection(SearchRequestBuilder request) {
    this.request = request;
  }

  /**
   * Project nested field in SELECT clause to InnerHit in NestedQueryBuilder
   *
   * @param fields list of field domain object
   */
  public void project(List<Field> fields, JoinType nestedJoinType) {
    if (isAnyNestedField(fields)) {
      initBoolQueryFilterIfNull();
      List<NestedQueryBuilder> nestedQueries = extractNestedQueries(query());

      if (nestedJoinType == JoinType.LEFT_OUTER_JOIN) {
        // for LEFT JOIN on nested field as right table, the query will have only one nested field,
        // so one path
        Map<String, List<String>> fieldNamesByPath = groupFieldNamesByPath(fields);

        if (fieldNamesByPath.size() > 1) {
          String message =
              StringUtils.format(
                  "only single nested field is allowed as right table for LEFT JOIN, found %s ",
                  fieldNamesByPath.keySet());

          throw new VerificationException(message);
        }

        Map.Entry<String, List<String>> pathToFields =
            fieldNamesByPath.entrySet().iterator().next();
        String path = pathToFields.getKey();
        List<String> fieldNames = pathToFields.getValue();
        buildNestedLeftJoinQuery(path, fieldNames);
      } else {

        groupFieldNamesByPath(fields)
            .forEach(
                (path, fieldNames) ->
                    buildInnerHit(fieldNames, findNestedQueryWithSamePath(nestedQueries, path)));
      }
    }
  }

  /**
   * Check via traditional for loop first to avoid lambda performance impact on all queries even
   * though those without nested field
   */
  private boolean isAnyNestedField(List<Field> fields) {
    for (Field field : fields) {
      if (field.isNested() && !field.isReverseNested()) {
        return true;
      }
    }
    return false;
  }

  private void initBoolQueryFilterIfNull() {
    if (request.request().source() == null || query() == null) {
      request.setQuery(boolQuery());
    }
    if (query().filter().isEmpty()) {
      query().filter(boolQuery());
    }
  }

  private Map<String, List<String>> groupFieldNamesByPath(List<Field> fields) {
    return fields.stream()
        .filter(Field::isNested)
        .filter(not(Field::isReverseNested))
        .collect(groupingBy(Field::getNestedPath, mapping(Field::getName, toList())));
  }

    /**
     * Why search for NestedQueryBuilder recursively?
     * Because
     * <ol>
     * <li>it was added and wrapped by BoolQuery when WHERE explained (far from here)
     * <li>InnerHit must be added to the NestedQueryBuilder related
     * </ol>
     * <p>
     * Either we store it to global data structure (which requires to be thread-safe or ThreadLocal)
     * or we peel off BoolQuery to find it (the way we followed here because recursion tree should be very thin).
     */
    private List<NestedQueryBuilder> extractNestedQueries(QueryBuilder query) {
        List<NestedQueryBuilder> result = new ArrayList<>();
        if (query instanceof NestedQueryBuilder) {
            result.add((NestedQueryBuilder) query);
        } else if (query instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQ = (BoolQueryBuilder) query;
            Stream.of(boolQ.filter(), boolQ.must(), boolQ.should()).
                    flatMap(Collection::stream).
                    forEach(q -> result.addAll(extractNestedQueries(q)));
        }
        return result;
    }

  private void buildInnerHit(List<String> fieldNames, NestedQueryBuilder query) {
    query.innerHit(
        new InnerHitBuilder()
            .setFetchSourceContext(
                new FetchSourceContext(true, fieldNames.toArray(new String[0]), null)));
  }

  /**
   * Why linear search? Because NestedQueryBuilder hides "path" field from any access. Assumption:
   * collected NestedQueryBuilder list should be very small or mostly only one.
   */
  private NestedQueryBuilder findNestedQueryWithSamePath(
      List<NestedQueryBuilder> nestedQueries, String path) {
    return nestedQueries.stream()
        .filter(query -> isSamePath(path, query))
        .findAny()
        .orElseGet(createEmptyNestedQuery(path));
  }

  private boolean isSamePath(String path, NestedQueryBuilder query) {
    return nestedQuery(path, query.query(), query.scoreMode()).equals(query);
  }

  /** Create a nested query with match all filter to place inner hits */
  private Supplier<NestedQueryBuilder> createEmptyNestedQuery(String path) {
    return () -> {
      NestedQueryBuilder nestedQuery = nestedQuery(path, matchAllQuery(), ScoreMode.None);
      ((BoolQueryBuilder) query().filter().get(0)).must(nestedQuery);
      return nestedQuery;
    };
  }

  private BoolQueryBuilder query() {
    return (BoolQueryBuilder) request.request().source().query();
  }

  private <T> Predicate<T> not(Predicate<T> predicate) {
    return predicate.negate();
  }

  private void buildNestedLeftJoinQuery(String path, List<String> fieldNames) {
    BoolQueryBuilder existsNestedQuery = boolQuery();
    existsNestedQuery.mustNot().add(nestedQuery(path, existsQuery(path), ScoreMode.None));

    NestedQueryBuilder matchAllNestedQuery = nestedQuery(path, matchAllQuery(), ScoreMode.None);
    buildInnerHit(fieldNames, matchAllNestedQuery);

    ((BoolQueryBuilder) query().filter().get(0)).should().add(existsNestedQuery);
    ((BoolQueryBuilder) query().filter().get(0)).should().add(matchAllNestedQuery);
  }
}
