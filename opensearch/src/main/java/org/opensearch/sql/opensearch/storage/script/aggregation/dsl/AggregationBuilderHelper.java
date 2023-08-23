/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static java.util.Collections.emptyMap;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.opensearch.sql.opensearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.opensearch.script.Script;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/** Abstract Aggregation Builder. */
@RequiredArgsConstructor
public class AggregationBuilderHelper {

  private final ExpressionSerializer serializer;

  /**
   * Build AggregationBuilder from Expression.
   *
   * @param expression Expression
   * @return AggregationBuilder
   */
  public <T> T build(
      Expression expression, Function<String, T> fieldBuilder, Function<Script, T> scriptBuilder) {
    if (expression instanceof ReferenceExpression) {
      String fieldName = ((ReferenceExpression) expression).getAttr();
      return fieldBuilder.apply(
          OpenSearchTextType.convertTextToKeyword(fieldName, expression.type()));
    } else if (expression instanceof FunctionExpression
        || expression instanceof LiteralExpression) {
      return scriptBuilder.apply(
          new Script(
              DEFAULT_SCRIPT_TYPE,
              EXPRESSION_LANG_NAME,
              serializer.serialize(expression),
              emptyMap()));
    } else {
      throw new IllegalStateException(
          String.format("metric aggregation doesn't support " + "expression %s", expression));
    }
  }
}
