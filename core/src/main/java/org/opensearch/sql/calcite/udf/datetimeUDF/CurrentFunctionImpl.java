/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Implementation of the now-like functions: CURRENT_TIMESTAMP, NOW, LOCALTIMESTAMP, LOCALTIME,
 * CURTIME, CURRENT_TIME, CURRENT_DATE, CUR_DATE
 *
 * <p>It returns the current date, time, or timestamp based on the specified return type.
 */
public class CurrentFunctionImpl extends ImplementorUDF {
  private final SqlTypeName returnType;

  public CurrentFunctionImpl(SqlTypeName returnType) {
    super(new CurrentFunctionImplementor(returnType), NullPolicy.ANY);
    this.returnType = returnType;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      RelDataType type =
          switch (returnType) {
            case DATE, TIME, TIMESTAMP -> OpenSearchTypeFactory.convertSqlTypeToRelDataType(
                returnType);
            default -> throw new IllegalArgumentException("Unsupported return type: " + returnType);
          };
      return opBinding.getTypeFactory().createTypeWithNullability(type, true);
    };
  }

  public static class CurrentFunctionImplementor implements NotNullImplementor {
    private final SqlTypeName returnType;

    public CurrentFunctionImplementor(SqlTypeName returnType) {
      super();
      this.returnType = returnType;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      return Expressions.call(
          CurrentFunctionImplementor.class,
          "current",
          Expressions.constant(returnType),
          Expressions.convert_(translator.getRoot(), Object.class));
    }

    public static Object current(SqlTypeName returnType, Object propertyContext) {
      FunctionProperties functionProperties =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);

      LocalDateTime now = DateTimeFunctions.formatNow(functionProperties.getQueryStartClock());
      return switch (returnType) {
        case DATE -> new ExprDateValue(now.toLocalDate()).valueForCalcite();
        case TIME -> new ExprTimeValue(now.toLocalTime()).valueForCalcite();
        case TIMESTAMP -> new ExprTimestampValue(now).valueForCalcite();
        default -> throw new IllegalArgumentException("Unsupported return type: " + returnType);
      };
    }
  }
}
