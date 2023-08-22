/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.utils.OperatorUtils;

/**
 * The definition of binary predicate function and, Accepts two Boolean values and produces a
 * Boolean. or, Accepts two Boolean values and produces a Boolean. xor, Accepts two Boolean values
 * and produces a Boolean. equalTo, Compare the left expression and right expression and produces a
 * Boolean.
 */
@UtilityClass
public class BinaryPredicateOperator {
  /**
   * Register Binary Predicate Function.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(and());
    repository.register(or());
    repository.register(xor());
    repository.register(equal());
    repository.register(notEqual());
    repository.register(less());
    repository.register(lte());
    repository.register(greater());
    repository.register(gte());
    repository.register(like());
    repository.register(notLike());
    repository.register(regexp());
  }

  /**
   * The and logic. A B A AND B TRUE TRUE TRUE TRUE FALSE FALSE TRUE NULL NULL TRUE MISSING MISSING
   * FALSE FALSE FALSE FALSE NULL FALSE FALSE MISSING FALSE NULL NULL NULL NULL MISSING MISSING
   * MISSING MISSING MISSING
   */
  private static Table<ExprValue, ExprValue, ExprValue> andTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_TRUE, LITERAL_TRUE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_TRUE, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_TRUE, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_FALSE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_NULL, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_MISSING, LITERAL_FALSE)
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_MISSING)
          .build();

  /**
   * The or logic. A B A AND B TRUE TRUE TRUE TRUE FALSE TRUE TRUE NULL TRUE TRUE MISSING TRUE FALSE
   * FALSE FALSE FALSE NULL NULL FALSE MISSING MISSING NULL NULL NULL NULL MISSING NULL MISSING
   * MISSING MISSING
   */
  private static Table<ExprValue, ExprValue, ExprValue> orTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_TRUE, LITERAL_TRUE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_FALSE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_NULL, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_MISSING, LITERAL_TRUE)
          .put(LITERAL_FALSE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_FALSE, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_NULL)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_MISSING)
          .build();

  /**
   * The xor logic. A B A AND B TRUE TRUE FALSE TRUE FALSE TRUE TRUE NULL TRUE TRUE MISSING TRUE
   * FALSE FALSE FALSE FALSE NULL NULL FALSE MISSING MISSING NULL NULL NULL NULL MISSING NULL
   * MISSING MISSING MISSING
   */
  private static Table<ExprValue, ExprValue, ExprValue> xorTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_TRUE, LITERAL_TRUE, LITERAL_FALSE)
          .put(LITERAL_TRUE, LITERAL_FALSE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_NULL, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_MISSING, LITERAL_TRUE)
          .put(LITERAL_FALSE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_FALSE, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_NULL)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_MISSING)
          .build();

  private static DefaultFunctionResolver and() {
    return define(
        BuiltinFunctionName.AND.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, andTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver or() {
    return define(
        BuiltinFunctionName.OR.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, orTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver xor() {
    return define(
        BuiltinFunctionName.XOR.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, xorTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver equal() {
    return define(
        BuiltinFunctionName.EQUAL.getName(),
        ExprCoreType.coreTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling((v1, v2) -> ExprBooleanValue.of(v1.equals(v2))),
                        BOOLEAN,
                        type,
                        type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver notEqual() {
    return define(
        BuiltinFunctionName.NOTEQUAL.getName(),
        ExprCoreType.coreTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling((v1, v2) -> ExprBooleanValue.of(!v1.equals(v2))),
                        BOOLEAN,
                        type,
                        type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver less() {
    return define(
        BuiltinFunctionName.LESS.getName(),
        ExprCoreType.coreTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling((v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) < 0)),
                        BOOLEAN,
                        type,
                        type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver lte() {
    return define(
        BuiltinFunctionName.LTE.getName(),
        ExprCoreType.coreTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling((v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) <= 0)),
                        BOOLEAN,
                        type,
                        type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver greater() {
    return define(
        BuiltinFunctionName.GREATER.getName(),
        ExprCoreType.coreTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling((v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) > 0)),
                        BOOLEAN,
                        type,
                        type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver gte() {
    return define(
        BuiltinFunctionName.GTE.getName(),
        ExprCoreType.coreTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling((v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) >= 0)),
                        BOOLEAN,
                        type,
                        type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver like() {
    return define(
        BuiltinFunctionName.LIKE.getName(),
        impl(nullMissingHandling(OperatorUtils::matches), BOOLEAN, STRING, STRING));
  }

  private static DefaultFunctionResolver regexp() {
    return define(
        BuiltinFunctionName.REGEXP.getName(),
        impl(nullMissingHandling(OperatorUtils::matchesRegexp), INTEGER, STRING, STRING));
  }

  private static DefaultFunctionResolver notLike() {
    return define(
        BuiltinFunctionName.NOT_LIKE.getName(),
        impl(
            nullMissingHandling(
                (v1, v2) -> UnaryPredicateOperator.not(OperatorUtils.matches(v1, v2))),
            BOOLEAN,
            STRING,
            STRING));
  }

  private static ExprValue lookupTableFunction(
      ExprValue arg1, ExprValue arg2, Table<ExprValue, ExprValue, ExprValue> table) {
    if (table.contains(arg1, arg2)) {
      return table.get(arg1, arg2);
    } else {
      return table.get(arg2, arg1);
    }
  }
}
