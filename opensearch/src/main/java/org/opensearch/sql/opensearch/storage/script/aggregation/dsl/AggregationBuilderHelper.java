/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static java.util.Collections.emptyMap;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.opensearch.sql.opensearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;

import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/** Abstract Aggregation Builder. */
@RequiredArgsConstructor
public class AggregationBuilderHelper {

  private final ExpressionSerializer serializer;

  /** Build Composite Builder from Expression. */
  public <T> T buildComposite(
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
          String.format("bucket aggregation doesn't support " + "expression %s", expression));
    }
  }

  /**
   * Build AggregationBuilder from Expression.
   *
   * @param expression Expression
   * @return AggregationBuilder
   */
  public AggregationBuilder build(
      Expression expression,
      Function<String, AggregationBuilder> fieldBuilder,
      Function<Script, AggregationBuilder> scriptBuilder) {
    if (expression instanceof ReferenceExpression) {
      String fieldName = ((ReferenceExpression) expression).getAttr();
      return fieldBuilder.apply(
          OpenSearchTextType.convertTextToKeyword(fieldName, expression.type()));
    } else if (expression instanceof FunctionExpression
        && ((FunctionExpression) expression)
            .getFunctionName()
            .equals(BuiltinFunctionName.NESTED.getName())) {
      List<Expression> args = ((FunctionExpression) expression).getArguments();
      // NestedAnalyzer has validated the number of arguments.
      // Here we can safety invoke args.getFirst().
      String fieldName = ((ReferenceExpression) args.getFirst()).getAttr();
      if (fieldName.contains("*")) {
        throw new IllegalArgumentException("Nested aggregation doesn't support multiple fields");
      }
      String path =
          args.size() == 2
              ? ((ReferenceExpression) args.get(1)).getAttr()
              : fieldName.substring(0, fieldName.lastIndexOf("."));
      AggregationBuilder subAgg =
          fieldBuilder.apply(OpenSearchTextType.convertTextToKeyword(fieldName, expression.type()));
      return AggregationBuilders.nested(path + "_nested", path).subAggregation(subAgg);
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
