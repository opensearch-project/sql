/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 * Base class to represent relevance queries that search multiple fields.
 *
 * @param <T> The builder class for the OpenSearch query.
 */
abstract class MultiFieldQuery<T extends QueryBuilder> extends RelevanceQuery<T> {

  public MultiFieldQuery(Map<String, QueryBuilderStep<T>> queryBuildActions) {
    super(queryBuildActions);
  }

  @Override
  public T createQueryBuilder(List<NamedArgumentExpression> arguments) {
    // Extract 'fields' and 'query'
    var fields =
        arguments.stream()
            .filter(a -> a.getArgName().equalsIgnoreCase("fields"))
            .findFirst()
            .orElseThrow(() -> new SemanticCheckException("'fields' parameter is missing."));

    var query =
        arguments.stream()
            .filter(a -> a.getArgName().equalsIgnoreCase("query"))
            .findFirst()
            .orElseThrow(() -> new SemanticCheckException("'query' parameter is missing"));

    var fieldsAndWeights =
        fields.getValue().valueOf().tupleValue().entrySet().stream()
            .collect(ImmutableMap.toImmutableMap(e -> e.getKey(), e -> e.getValue().floatValue()));

    return createBuilder(fieldsAndWeights, query.getValue().valueOf().stringValue());
  }

  protected abstract T createBuilder(ImmutableMap<String, Float> fields, String query);

  /**
   * Build multi-fields relevance query builder based on Calcite function's operands. For
   * MultiFieldQuery, fields with weights and query string parameter are required.
   *
   * @param fieldsRexCall Calcite MAP RexCall that wraps multi-fields and corresponding weights
   * @param query String query to search
   * @param optionalArguments Map contains optional relevance query argument key value pairs
   * @return Final QueryBuilder
   */
  public T build(RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
    List<RexNode> fieldAndWeightNodes = fieldsRexCall.getOperands();
    ImmutableMap<String, Float> fields =
        IntStream.range(0, fieldsRexCall.getOperands().size() / 2)
            .map(i -> i * 2)
            .mapToObj(
                i -> {
                  RexLiteral fieldLiteral = (RexLiteral) fieldAndWeightNodes.get(i);
                  RexLiteral weightLiteral = (RexLiteral) fieldAndWeightNodes.get(i + 1);
                  String field =
                      ((NlsString) Objects.requireNonNull(fieldLiteral.getValue())).getValue();
                  Float weight =
                      ((Double) Objects.requireNonNull(weightLiteral.getValue())).floatValue();
                  return Map.entry(field, weight);
                })
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    T queryBuilder = createBuilder(fields, query);

    return applyArguments(queryBuilder, optionalArguments);
  }
}
