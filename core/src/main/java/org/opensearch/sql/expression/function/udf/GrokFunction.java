/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.parse.GrokExpression;

public final class GrokFunction extends ImplementorUDF {

  public GrokFunction() {
    super(new GrokImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        SqlTypeUtil.createMapType(
            TYPE_FACTORY,
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            false));
  }

  public static class GrokImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(GrokFunction.GrokImplementor.class, "grok", translatedOperands);
    }

    public static Map<String, String> grok(String input, String regex) {
      if (input == null) {
        return Collections.EMPTY_MAP;
      }
      LiteralExpression inputExpr = DSL.literal(input);
      LiteralExpression regexExpr = DSL.literal(regex);
      List<String> namedFields = GrokExpression.getNamedGroupCandidates(regex);
      return namedFields.stream()
          .map(
              namedField -> {
                GrokExpression grokExpression =
                    new GrokExpression(inputExpr, regexExpr, DSL.literal(namedField));
                ExprValue parsedValue = grokExpression.parseValue(inputExpr.valueOf());
                return Pair.of(namedField, parsedValue.stringValue());
              })
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }
  }
}
