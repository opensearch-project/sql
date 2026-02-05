/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.utils.ParseUtils;

public final class ParseFunction extends ImplementorUDF {

  public ParseFunction() {
    super(new ParseImplementor(), NullPolicy.NONE);
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

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_STRING_STRING;
  }

  public static class ParseImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(ParseFunction.ParseImplementor.class, "parse", translatedOperands);
    }

    public static Map<String, String> parse(String input, String regex, String parseMethod) {
      ParseMethod method = ParseMethod.valueOf(parseMethod.toUpperCase(Locale.ROOT));
      List<String> namedFields =
          ParseUtils.getNamedGroupCandidates(method, regex, Collections.emptyMap());
      if (input == null) {
        return namedFields.stream().collect(Collectors.toMap(element -> element, element -> ""));
      }
      LiteralExpression inputExpr = DSL.literal(input);
      LiteralExpression regexExpr = DSL.literal(regex);
      return namedFields.stream()
          .map(
              namedField -> {
                ParseExpression parseExpr =
                    ParseUtils.createParseExpression(
                        method, inputExpr, regexExpr, DSL.literal(namedField));
                ExprValue parsedValue = parseExpr.parseValue(inputExpr.valueOf());
                return Pair.of(namedField, parsedValue.stringValue());
              })
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }
  }
}
