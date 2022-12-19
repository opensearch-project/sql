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
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.implWithProperties;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandlingWithProperties;
import static org.opensearch.sql.utils.DateTimeUtils.extractDateTime;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.SerializableBiFunction;
import org.opensearch.sql.utils.OperatorUtils;

/**
 * The definition of binary predicate function
 * and, Accepts two Boolean values and produces a Boolean.
 * or,  Accepts two Boolean values and produces a Boolean.
 * xor, Accepts two Boolean values and produces a Boolean.
 * equalTo, Compare the left expression and right expression and produces a Boolean.
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
   * The and logic.
   * A       B       A AND B
   * TRUE    TRUE    TRUE
   * TRUE    FALSE   FALSE
   * TRUE    NULL    NULL
   * TRUE    MISSING MISSING
   * FALSE   FALSE   FALSE
   * FALSE   NULL    FALSE
   * FALSE   MISSING FALSE
   * NULL    NULL    NULL
   * NULL    MISSING MISSING
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
   * The or logic.
   * A       B       A AND B
   * TRUE    TRUE    TRUE
   * TRUE    FALSE   TRUE
   * TRUE    NULL    TRUE
   * TRUE    MISSING TRUE
   * FALSE   FALSE   FALSE
   * FALSE   NULL    NULL
   * FALSE   MISSING MISSING
   * NULL    NULL    NULL
   * NULL    MISSING NULL
   * MISSING MISSING MISSING
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
   * The xor logic.
   * A       B       A AND B
   * TRUE    TRUE    FALSE
   * TRUE    FALSE   TRUE
   * TRUE    NULL    TRUE
   * TRUE    MISSING TRUE
   * FALSE   FALSE   FALSE
   * FALSE   NULL    NULL
   * FALSE   MISSING MISSING
   * NULL    NULL    NULL
   * NULL    MISSING NULL
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
    return FunctionDSL.define(BuiltinFunctionName.AND.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, andTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver or() {
    return FunctionDSL.define(BuiltinFunctionName.OR.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, orTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver xor() {
    return FunctionDSL.define(BuiltinFunctionName.XOR.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, xorTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver equal() {
    return compareImpl(BuiltinFunctionName.EQUAL.getName(),
        (Comparable v1, Comparable v2) -> v1.equals(v2));
  }

  private static DefaultFunctionResolver notEqual() {
    return compareImpl(BuiltinFunctionName.NOTEQUAL.getName(),
        (Comparable v1, Comparable v2) -> !v1.equals(v2));
  }

  private static DefaultFunctionResolver less() {
    return compareImpl(BuiltinFunctionName.LESS.getName(),
        (Comparable v1, Comparable v2) -> v1.compareTo(v2) < 0);
  }

  private static DefaultFunctionResolver lte() {
    return compareImpl(BuiltinFunctionName.LTE.getName(),
        (Comparable v1, Comparable v2) -> v1.compareTo(v2) <= 0);
  }

  private static DefaultFunctionResolver greater() {
    return compareImpl(BuiltinFunctionName.GREATER.getName(),
        (Comparable v1, Comparable v2) -> v1.compareTo(v2) > 0);
  }

  private static DefaultFunctionResolver gte() {
    return compareImpl(BuiltinFunctionName.GTE.getName(),
        (Comparable v1, Comparable v2) -> v1.compareTo(v2) >= 0);
  }

  private static DefaultFunctionResolver compareImpl(
      FunctionName function, SerializableBiFunction<Comparable, Comparable, Boolean> comparator) {
    return FunctionDSL.define(function,
        Stream.concat(
            ExprCoreType.coreTypes().stream()
                .map(type -> impl(nullMissingHandling(
                    (v1, v2) -> ExprBooleanValue.of(comparator.apply(v1, v2))),
                        BOOLEAN, type, type)),
            permuteTemporalTypesByPairs().stream()
                .map(pair -> implWithProperties(nullMissingHandlingWithProperties(
                    (fp, v1, v2) -> ExprBooleanValue.of(comparator.apply(
                            extractDateTime(v1, fp), extractDateTime(v2, fp)))),
                        BOOLEAN, pair.getLeft(), pair.getRight())))
        .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver like() {
    return FunctionDSL.define(BuiltinFunctionName.LIKE.getName(),
        impl(nullMissingHandling(OperatorUtils::matches), BOOLEAN, STRING, STRING));
  }

  private static DefaultFunctionResolver regexp() {
    return FunctionDSL.define(BuiltinFunctionName.REGEXP.getName(),
        impl(nullMissingHandling(OperatorUtils::matchesRegexp), INTEGER, STRING, STRING));
  }

  private static DefaultFunctionResolver notLike() {
    return FunctionDSL.define(BuiltinFunctionName.NOT_LIKE.getName(), impl(nullMissingHandling(
            (v1, v2) -> UnaryPredicateOperator.not(OperatorUtils.matches(v1, v2))),
            BOOLEAN, STRING, STRING));
  }

  private static ExprValue lookupTableFunction(ExprValue arg1, ExprValue arg2,
                                               Table<ExprValue, ExprValue, ExprValue> table) {
    if (table.contains(arg1, arg2)) {
      return table.get(arg1, arg2);
    } else {
      return table.get(arg2, arg1);
    }
  }

  private static List<Pair<ExprCoreType, ExprCoreType>> permuteTemporalTypesByPairs() {
    return List.of(
        Pair.of(DATE, TIME), Pair.of(DATE, DATETIME), Pair.of(DATE, TIMESTAMP),
        Pair.of(TIME, DATE), Pair.of(TIME, DATETIME), Pair.of(TIME, TIMESTAMP),
        Pair.of(DATETIME, TIME), Pair.of(DATETIME, DATE), Pair.of(DATETIME, TIMESTAMP),
        Pair.of(TIMESTAMP, TIME), Pair.of(TIMESTAMP, DATE), Pair.of(TIMESTAMP, DATETIME)
    );
  }
}
