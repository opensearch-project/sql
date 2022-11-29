/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.nested;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

@UtilityClass
public class NestedFunctions {
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(nested());
  }

  private static DefaultFunctionResolver nested() {
    return FunctionDSL.define(BuiltinFunctionName.NESTED.getName(),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), BYTE, BYTE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), SHORT, SHORT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), INTEGER, INTEGER),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), LONG, LONG),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), FLOAT, FLOAT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), DOUBLE, DOUBLE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), STRING, STRING),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), BOOLEAN, BOOLEAN),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), DATE, DATE),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), TIME, TIME),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), DATETIME, DATETIME),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), TIMESTAMP, TIMESTAMP),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), INTERVAL, INTERVAL),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), STRUCT, STRUCT),
        FunctionDSL.impl(
            FunctionDSL.nullMissingHandling(NestedFunctions::nested_returns_val), ARRAY, ARRAY));
  }

  private ExprValue nested_returns_val(ExprValue field) {
    return field;
  }
}
