/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.gson;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
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
      if (args[0] instanceof List<?> values) {
        return values.stream().map(value -> cast(value, elementType)).toList();
      }
      JsonArray values = gson.fromJson(String.valueOf(args[0]), JsonArray.class);
      if (values == null) {
        return List.of();
      }
      return StreamSupport.stream(values.spliterator(), false)
          .map(value -> cast(value, elementType))
          .toList();
    } catch (JsonSyntaxException e) {
      return List.of();
    }
  }

  private static Object cast(Object value, SqlTypeName elementType) {
    if (value instanceof JsonElement element) {
      if (element.isJsonNull()) {
        return elementType == SqlTypeName.VARCHAR ? "null" : null;
      }
      if (element.isJsonArray() || element.isJsonObject()) {
        return element.toString();
      }
      value =
          element.getAsJsonPrimitive().isNumber() ? element.getAsNumber() : element.getAsString();
    }
    if (value == null) {
      return null;
    }
    // Match Calcite SAFE_CAST semantics for runtime values whose type is unknown during planning.
    try {
      return switch (elementType) {
        case DOUBLE -> SqlFunctions.toDouble(value);
        case VARCHAR ->
            value instanceof List<?> || value instanceof java.util.Map<?, ?>
                ? gson.toJson(value)
                : String.valueOf(value);
        case DECIMAL -> SqlFunctions.toBigDecimal(value);
        default -> value;
      };
    } catch (RuntimeException e) {
      return null;
    }
  }
}
