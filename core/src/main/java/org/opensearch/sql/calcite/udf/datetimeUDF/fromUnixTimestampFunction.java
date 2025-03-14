package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.DATE_HANDLERS;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedString;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;

/**
 * DOUBLE -> DATETIME DOUBLE, STRING -> STRING Mimic implementation from
 * DATETIMEFUNCTIONS::from_unixtime
 */
public class fromUnixTimestampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length == 1) {
      // Double input
      Object value = args[0];
      if (!(value instanceof Number)) {
        throw new IllegalArgumentException(
            "If only 1 argument for from_unixtimestamp function, then it should be number.");

      } else {
        double input = ((Number) value).doubleValue();
        LocalDateTime localDateTime =
            LocalDateTime.ofInstant(
                    InstantUtils.fromEpochMills((long) Math.floor(input)), ZoneOffset.UTC)
                .withNano((int) ((input % 1) * 1E9));
        return java.sql.Timestamp.valueOf(localDateTime);
      }
    } else if (args.length == 2) {
      Object value = args[0];
      Object target = args[1];
      double input = ((Number) value).doubleValue();
      LocalDateTime localDateTime =
          LocalDateTime.ofInstant(
                  InstantUtils.fromEpochMills((long) Math.floor(input)), ZoneOffset.UTC)
              .withNano((int) ((input % 1) * 1E9));
      ExprStringValue exprValue = new ExprStringValue(target.toString());
      return getFormattedString(exprValue, DATE_HANDLERS, localDateTime).stringValue();
    } else {
      throw new IllegalArgumentException("Too many arguments for from_unixtimestamp function");
    }
  }

  public static SqlReturnTypeInference interReturnTypes() {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      // Get argument types
      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      if (argTypes.size() == 1) {
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      }
      return typeFactory.createSqlType(SqlTypeName.CHAR);
    };
  }
}
