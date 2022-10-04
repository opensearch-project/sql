/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.convert;

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
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;

@UtilityClass
public class TypeOfOperator {
  /**
   * Register TypeOf Operator.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(typeof());
  }

  // Auxiliary function useful for debugging
  private static DefaultFunctionResolver typeof() {
    return FunctionDSL.define(BuiltinFunctionName.TYPEOF.getName(),
        impl(TypeOfOperator::exprTypeOf, STRING, ARRAY),
        impl(TypeOfOperator::exprTypeOf, STRING, BOOLEAN),
        impl(TypeOfOperator::exprTypeOf, STRING, BYTE),
        impl(TypeOfOperator::exprTypeOf, STRING, DATE),
        impl(TypeOfOperator::exprTypeOf, STRING, DATETIME),
        impl(TypeOfOperator::exprTypeOf, STRING, DOUBLE),
        impl(TypeOfOperator::exprTypeOf, STRING, FLOAT),
        impl(TypeOfOperator::exprTypeOf, STRING, INTEGER),
        impl(TypeOfOperator::exprTypeOf, STRING, INTERVAL),
        impl(TypeOfOperator::exprTypeOf, STRING, LONG),
        impl(TypeOfOperator::exprTypeOf, STRING, SHORT),
        impl(TypeOfOperator::exprTypeOf, STRING, STRING),
        impl(TypeOfOperator::exprTypeOf, STRING, STRUCT),
        impl(TypeOfOperator::exprTypeOf, STRING, TIME),
        impl(TypeOfOperator::exprTypeOf, STRING, TIMESTAMP),
        impl(TypeOfOperator::exprTypeOf, STRING, UNDEFINED),
        impl(TypeOfOperator::exprTypeOf, STRING, UNKNOWN)
      );
  }

  private static ExprValue exprTypeOf(ExprValue input) {
    return new ExprStringValue(input.type().typeName().toUpperCase());
  }
}
