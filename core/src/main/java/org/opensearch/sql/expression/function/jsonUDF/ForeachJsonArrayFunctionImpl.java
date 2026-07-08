/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.gson;

import com.google.gson.JsonSyntaxException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Converts a JSON array string to a Calcite enumerable array for foreach collection modes. */
public class ForeachJsonArrayFunctionImpl extends ImplementorUDF {
  public ForeachJsonArrayFunctionImpl() {
    super(new ForeachJsonArrayImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      SqlTypeName elementTypeName =
          SqlTypeName.valueOf(opBinding.getOperandLiteralValue(1, String.class));
      return SqlTypeUtil.createArrayType(
          opBinding.getTypeFactory(),
          opBinding
              .getTypeFactory()
              .createTypeWithNullability(
                  opBinding.getTypeFactory().createSqlType(elementTypeName), true),
          true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class ForeachJsonArrayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(ForeachJsonArrayFunctionImpl.class, "eval", Object[].class),
          translatedOperands);
    }
  }

  public static Object eval(Object... args) {
    if (args.length != 2 || args[0] == null) {
      return null;
    }
    SqlTypeName elementType = SqlTypeName.valueOf(String.valueOf(args[1]));
    try {
      List<?> values =
          args[0] instanceof List<?> list
              ? list
              : gson.fromJson(String.valueOf(args[0]), List.class);
      return values == null
          ? null
          : values.stream().map(value -> cast(value, elementType)).toList();
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  private static Object cast(Object value, SqlTypeName elementType) {
    if (value == null) {
      return null;
    }
    return switch (elementType) {
      case DOUBLE -> ((Number) value).doubleValue();
      case VARCHAR -> String.valueOf(value);
      case DECIMAL -> BigDecimal.valueOf(((Number) value).doubleValue());
      default -> value;
    };
  }
}
