/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.nullableDateUDT;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.nullableTimestampUDT;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatDate;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestamp;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.convertToTemporalAmount;

import com.google.common.collect.ImmutableList;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;

public class DateAddSubFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    //UserDefinedFunctionUtils.validateArgumentCount("DATE_ADD / DATE_SUB", 6, args.length, false);
    /*
    UserDefinedFunctionUtils.validateArgumentTypes(
        Arrays.asList(args),
        ImmutableList.of(
            TimeUnit.class,
            Number.class,
            Number.class,
            SqlTypeName.class,
            Boolean.class,
            SqlTypeName.class),
        Collections.nCopies(6, true));

     */

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    TimeUnit unit = (TimeUnit) args[0];
    long interval = ((Number) args[1]).longValue();
    Object argBase = args[2];
    SqlTypeName sqlTypeName = (SqlTypeName) args[3];
    boolean isAdd = (Boolean) args[4];
    SqlTypeName returnSqlType = (SqlTypeName) args[5];
    Instant base = InstantUtils.convertToInstant(argBase, sqlTypeName, false);
    /*
    Instant base =
        switch (sqlTypeName) {
          case DATE ->
          // Convert it to milliseconds
          InstantUtils.fromInternalDate(argBase.intValue());
          case TIME ->
          // Add an offset of today's date at 00:00:00
          InstantUtils.fromInternalTime(argBase.intValue());
          case TIMESTAMP -> InstantUtils.fromEpochMills(argBase.longValue());
          default -> throw new IllegalArgumentException(
              "Invalid argument type. Must be DATE, TIME, or TIMESTAMP, but got " + sqlTypeName);
        };

     */
    ExprValue resultDatetime =
        DateTimeFunctions.exprDateApplyInterval(
            new FunctionProperties(),
            new ExprTimestampValue(base),
            convertToTemporalAmount(interval, unit),
            isAdd);
    Instant resultInstant = resultDatetime.timestampValue();
    if (returnSqlType == SqlTypeName.TIMESTAMP) {
      return formatTimestamp(LocalDateTime.ofInstant(resultInstant, ZoneOffset.UTC));
      //return Timestamp.valueOf(LocalDateTime.ofInstant(resultInstant, ZoneOffset.UTC));
    } else {
      return formatDate(LocalDateTime.ofInstant(resultInstant, ZoneOffset.UTC).toLocalDate());
      //return java.sql.Date.valueOf(
      //    LocalDateTime.ofInstant(resultInstant, ZoneOffset.UTC).toLocalDate());
    }
  }

  public static SqlReturnTypeInference getReturnTypeForAddOrSubDate() {
    return opBinding -> {
      RelDataType operandType0 = opBinding.getOperandType(6);
      SqlTypeName typeName = operandType0.getSqlTypeName();
      if (typeName == SqlTypeName.TIMESTAMP) {
        return nullableTimestampUDT;
      } else if (typeName == SqlTypeName.DATE) {
        return nullableDateUDT;
      }
      return opBinding.getTypeFactory().createSqlType(typeName);
    };
  }
}
