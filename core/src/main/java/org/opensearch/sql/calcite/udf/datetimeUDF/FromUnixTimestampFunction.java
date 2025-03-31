/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getNullableTimestampUDTWithCharset;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprFromUnixTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprFromUnixTimeFormat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;

/**
 * DOUBLE -> DATETIME DOUBLE, STRING -> STRING Mimic implementation from
 * DATETIMEFUNCTIONS::from_unixtime
 */
public class FromUnixTimestampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    if (args.length == 1) {
      // Double input
      Object value = args[0];
      if (!(value instanceof Number)) {
        throw new IllegalArgumentException(
            "If only 1 argument for from_unixtimestamp function, then it should be number.");

      } else {
        double input = ((Number) value).doubleValue();
        return exprFromUnixTime(new ExprDoubleValue(input)).valueForCalcite();
      }
    } else if (args.length == 2) {
      Object value = args[0];
      Object target = args[1];
      return exprFromUnixTimeFormat(
              new ExprDoubleValue((Number) value), new ExprStringValue((String) target))
          .valueForCalcite();
    } else {
      throw new IllegalArgumentException("Too many arguments for from_unixtimestamp function");
    }
  }

  public static SqlReturnTypeInference interReturnTypes() {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      if (argTypes.size() == 1) {
        return getNullableTimestampUDTWithCharset();
      }
      return typeFactory.createSqlType(SqlTypeName.CHAR);
    };
  }
}
