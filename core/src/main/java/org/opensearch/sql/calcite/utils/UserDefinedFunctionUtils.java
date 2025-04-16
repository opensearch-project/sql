/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.*;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Optionality;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;

public class UserDefinedFunctionUtils {
  public static SqlReturnTypeInference INTEGER_FORCE_NULLABLE =
      ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  public static RelDataType nullableTimeUDT = TYPE_FACTORY.createUDT(EXPR_TIME, true);
  public static RelDataType nullableDateUDT = TYPE_FACTORY.createUDT(EXPR_DATE, true);
  public static RelDataType nullableTimestampUDT =
      TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, true);

  public static SqlReturnTypeInference timestampInference =
      ReturnTypes.explicit(nullableTimestampUDT);

  public static SqlReturnTypeInference timeInference = ReturnTypes.explicit(nullableTimeUDT);

  public static SqlReturnTypeInference dateInference = ReturnTypes.explicit(nullableDateUDT);

  public static RelBuilder.AggCall TransferUserDefinedAggFunction(
      Class<? extends UserDefinedAggFunction> UDAF,
      String functionName,
      SqlReturnTypeInference returnType,
      List<RexNode> fields,
      List<RexNode> argList,
      RelBuilder relBuilder) {
    SqlUserDefinedAggFunction sqlUDAF =
        new SqlUserDefinedAggFunction(
            new SqlIdentifier(functionName, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            returnType,
            null,
            null,
            AggregateFunctionImpl.create(UDAF),
            false,
            false,
            Optionality.FORBIDDEN);
    List<RexNode> addArgList = new ArrayList<>(fields);
    addArgList.addAll(argList);
    return relBuilder.aggregateCall(sqlUDAF, addArgList);
  }

  public static SqlOperator TransferUserDefinedFunction(
      Class<? extends UserDefinedFunction> UDF,
      String functionName,
      SqlReturnTypeInference returnType) {
    final ScalarFunction udfFunction =
        ScalarFunctionImpl.create(Types.lookupMethod(UDF, "eval", Object[].class));
    SqlIdentifier udfLtrimIdentifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    return new SqlUserDefinedFunction(
        udfLtrimIdentifier,
        SqlKind.OTHER_FUNCTION,
        returnType,
        InferTypes.ANY_NULLABLE,
        null,
        udfFunction);
  }

  static SqlReturnTypeInference getReturnTypeInferenceForArray() {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      // Get argument types
      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      RelDataType firstArgType = argTypes.getFirst();
      return createArrayType(typeFactory, firstArgType, true);
    };
  }

  static List<Integer> transferStringExprToDateValue(String timeExpr) {
    try {
      if (timeExpr.contains(":")) {
        // A timestamp
        LocalDateTime localDateTime =
            LocalDateTime.parse(timeExpr, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
        return List.of(
            localDateTime.getYear(), localDateTime.getMonthValue(), localDateTime.getDayOfMonth());
      } else {
        LocalDate localDate =
            LocalDate.parse(timeExpr, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
        return List.of(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
      }
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format("date:%s in unsupported format, please use 'yyyy-MM-dd'", timeExpr));
    }
  }

  /**
   * Check whether a function gets enough arguments.
   *
   * @param funcName the name of the function
   * @param expectedArguments the number of expected arguments
   * @param actualArguments the number of actual arguments
   * @param exactMatch whether the number of actual arguments should precisely match the number of
   *     expected arguments. If false, it suffices as long as the number of actual number of
   *     arguments is not smaller that the number of expected arguments.
   * @throws IllegalArgumentException if the argument length does not match the expected one
   */
  public static void validateArgumentCount(
      String funcName, int expectedArguments, int actualArguments, boolean exactMatch) {
    if (exactMatch) {
      if (actualArguments != expectedArguments) {
        throw new IllegalArgumentException(
            String.format(
                "Mismatch arguments: function %s expects %d arguments, but got %d",
                funcName, expectedArguments, actualArguments));
      }
    } else {
      if (actualArguments < expectedArguments) {
        throw new IllegalArgumentException(
            String.format(
                "Mismatch arguments: function %s expects at least %d arguments, but got %d",
                funcName, expectedArguments, actualArguments));
      }
    }
  }

  /**
   * Validates that the given list of objects matches the given list of types.
   *
   * <p>This function first checks if the sizes of the two lists match. If not, it throws an {@code
   * IllegalArgumentException}. Then, it iterates through the lists and checks if each object is an
   * instance of the corresponding type. If any object is not of the expected type, it throws an
   * {@code IllegalArgumentException} with a descriptive message.
   *
   * @param objects the list of objects to validate
   * @param types the list of expected types
   * @throws IllegalArgumentException if the sizes of the lists do not match or if any object is not
   *     an instance of the corresponding type
   */
  public static void validateArgumentTypes(List<Object> objects, List<Class<?>> types) {
    validateArgumentTypes(objects, types, Collections.nCopies(types.size(), false));
  }

  public static void validateArgumentTypes(
      List<Object> objects, List<Class<?>> types, boolean nullable) {
    validateArgumentTypes(objects, types, Collections.nCopies(types.size(), nullable));
  }

  public static void validateArgumentTypes(
      List<Object> objects, List<Class<?>> types, List<Boolean> nullables) {
    if (objects.size() < types.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Mismatch in the number of objects and types. Got %d objects and %d types",
              objects.size(), types.size()));
    }
    for (int i = 0; i < types.size(); i++) {
      if (objects.get(i) == null && nullables.get(i)) {
        continue;
      }
      if (!types.get(i).isInstance(objects.get(i))) {
        throw new IllegalArgumentException(
            String.format(
                "Object at index %d is not of type %s (Got %s)",
                i,
                types.get(i).getName(),
                objects.get(i) == null ? "null" : objects.get(i).getClass().getName()));
      }
    }
  }

  /** Check whether the given array contains null values. */
  public static boolean containsNull(Object[] objects) {
    return Arrays.stream(objects).anyMatch(Objects::isNull);
  }

  public static String formatTimestampWithoutUnnecessaryNanos(LocalDateTime localDateTime) {
    String base = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    int nano = localDateTime.getNano();
    if (nano == 0) return base;

    String nanoStr = String.format(Locale.ENGLISH, "%09d", nano);
    nanoStr = nanoStr.replaceFirst("0+$", "");
    if (!nanoStr.isEmpty()) {
      return base + "." + nanoStr;
    }
    return base;
  }

  public static SqlTypeName transferDateRelatedTimeName(RexNode candidate) {
    RelDataType type = candidate.getType();
    return OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(type);
  }

  // TODO: pass the function properties directly to the UDF instead of string
  public static FunctionProperties restoreFunctionProperties(Object timestampStr) {
    String expression = (String) timestampStr;
    Instant parsed = Instant.parse(expression);
    FunctionProperties functionProperties =
        new FunctionProperties(parsed, ZoneId.systemDefault(), QueryType.PPL);
    return functionProperties;
  }
}
