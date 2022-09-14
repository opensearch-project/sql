/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.script.filter;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * This class creates promql filter query from the filter expression function.
 */
@RequiredArgsConstructor
public class PromQlFilterQueryBuilder extends ExpressionNodeVisitor<PromFilterQuery, Object> {

  /**
   * Type converting map.
   */
  private final Map<FunctionName, Function<LiteralExpression, ExprValue>> castMap = ImmutableMap
      .<FunctionName, Function<LiteralExpression, ExprValue>>builder()
      .put(BuiltinFunctionName.CAST_TO_STRING.getName(), expr -> {
        if (!expr.type().equals(ExprCoreType.STRING)) {
          return new ExprStringValue(String.valueOf(expr.valueOf(null).value()));
        } else {
          return expr.valueOf(null);
        }
      })
      .put(BuiltinFunctionName.CAST_TO_BYTE.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprByteValue(expr.valueOf(null).byteValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprByteValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprByteValue(Byte.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_SHORT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprShortValue(expr.valueOf(null).shortValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprShortValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprShortValue(Short.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_INT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprIntegerValue(expr.valueOf(null).integerValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprIntegerValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprIntegerValue(Integer.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_LONG.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprLongValue(expr.valueOf(null).longValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprLongValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprLongValue(Long.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_FLOAT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprFloatValue(expr.valueOf(null).floatValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprFloatValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprFloatValue(Float.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DOUBLE.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprDoubleValue(expr.valueOf(null).doubleValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprDoubleValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprDoubleValue(Double.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_BOOLEAN.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return expr.valueOf(null).doubleValue() == 1
              ? ExprBooleanValue.of(true) : ExprBooleanValue.of(false);
        } else if (expr.type().equals(ExprCoreType.STRING)) {
          return ExprBooleanValue.of(Boolean.valueOf(expr.valueOf(null).stringValue()));
        } else {
          return expr.valueOf(null);
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DATE.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprDateValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprDateValue(expr.valueOf(null).dateValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_TIME.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprTimeValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprTimeValue(expr.valueOf(null).timeValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DATETIME.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprDatetimeValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprDatetimeValue(expr.valueOf(null).datetimeValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprTimestampValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprTimestampValue(expr.valueOf(null).timestampValue());
        }
      })
      .build();

  /**
   * Build OpenSearch filter query from expression.
   *
   * @param expr expression
   * @return query
   */
  public PromFilterQuery build(Expression expr) {
    return expr.accept(this, null);
  }

  @Override
  public PromFilterQuery visitFunction(FunctionExpression func, Object context) {
    PromFilterQuery promFilterQuery = new PromFilterQuery();
    if (func.getFunctionName().getFunctionName().equals("query_range")) {
      visitTableFunction(promFilterQuery, func, context);
    }
    return promFilterQuery;
  }

  private void visitTableFunction(PromFilterQuery promFilterQuery, FunctionExpression func,
                                  Object context) {
    func.getArguments().forEach(arg -> {
      String argName = ((NamedArgumentExpression) arg).getArgName();
      Expression argValue = ((NamedArgumentExpression) arg).getValue();
      ExprValue literalValue = argValue instanceof LiteralExpression ? argValue
          .valueOf(null) : cast((FunctionExpression) argValue);
      switch (argName) {
        case "query":
          promFilterQuery.setPromQl(new StringBuilder((String) value(literalValue)));
          break;
        case "startTime":
          promFilterQuery.setStartTime((Long) value(literalValue));
          break;
        case "endTime":
          promFilterQuery.setEndTime((Long) value(literalValue));
          break;
        case "step":
          promFilterQuery.setStep(((Integer) value(literalValue)).toString());
          break;
        default:
          break;
      }
    });
  }

  private ExprValue cast(FunctionExpression castFunction) {
    return castMap.get(castFunction.getFunctionName()).apply(
        (LiteralExpression) castFunction.getArguments().get(0));
  }

  private Object value(ExprValue literal) {
    if (literal.type().equals(ExprCoreType.TIMESTAMP)) {
      return literal.timestampValue().toEpochMilli();
    } else {
      return literal.value();
    }
  }
}
