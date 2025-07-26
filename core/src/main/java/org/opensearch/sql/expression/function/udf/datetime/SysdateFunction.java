/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.prependFunctionProperties;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Returns the timestamp at which it <b>executes</b>. It differs from the behavior for NOW(), which
 * returns a constant time that indicates the time at which the statement began to execute. If an
 * argument is given, it specifies a fractional seconds precision from 0 to 6, the return value
 * includes a fractional seconds part of that many digits.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>() -> TIMESTAMP
 *   <li>(INTEGER) -> TIMESTAMP
 * </ul>
 */
public class SysdateFunction extends ImplementorUDF {

  public SysdateFunction() {
    super(new SysdateImplementor(), NullPolicy.ANY);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.OPTIONAL_INTEGER;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE;
  }

  public static class SysdateImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      List<Expression> operandsWithProperties =
          prependFunctionProperties(translatedOperands, translator);
      return Expressions.call(SysdateImplementor.class, "sysdate", operandsWithProperties);
    }

    public static String sysdate(FunctionProperties properties) {
      var localDateTime = DateTimeFunctions.formatNow(properties.getSystemClock(), 0);
      return (String) new ExprTimestampValue(localDateTime).valueForCalcite();
    }

    public static String sysdate(FunctionProperties properties, int precision) {
      var localDateTime = DateTimeFunctions.formatNow(properties.getSystemClock(), precision);
      return (String) new ExprTimestampValue(localDateTime).valueForCalcite();
    }
  }
}
