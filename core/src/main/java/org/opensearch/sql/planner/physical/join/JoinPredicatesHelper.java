/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

@UtilityClass
public class JoinPredicatesHelper {

  private static boolean instanceOf(Expression function, BuiltinFunctionName functionName) {
    return function instanceof FunctionExpression
        && ((FunctionExpression) function).getFunctionName().equals(functionName.getName());
  }

  private static boolean isValidJoinPredicate(FunctionExpression predicate) {
    BuiltinFunctionName builtinFunctionName = BuiltinFunctionName.of(predicate.getFunctionName());
    switch (builtinFunctionName) {
      case AND:
      case OR:
      case EQUAL:
      case NOTEQUAL:
      case LESS:
      case LTE:
      case GREATER:
      case GTE:
        return true;
      default:
        return false;
    }
  }

  public static ImmutablePair<Expression, Expression> extractJoinKeys(
      FunctionExpression predicate) {
    if (isValidJoinPredicate(predicate)) {
      throw new SemanticCheckException(
          StringUtils.format(
              "Join condition {} is an invalid function",
              predicate.getFunctionName().getFunctionName()));
    } else {
      return ImmutablePair.of(
          predicate.getArguments().getFirst(), predicate.getArguments().getLast());
    }
  }

  public static List<Expression> splitConjunctivePredicates(Expression condition) {
    if (JoinPredicatesHelper.isAnd(condition)) {
      return Stream.concat(
              splitConjunctivePredicates(((FunctionExpression) condition).getArguments().getFirst())
                  .stream(),
              splitConjunctivePredicates(((FunctionExpression) condition).getArguments().getLast())
                  .stream())
          .collect(Collectors.toList());
    } else {
      return ImmutableList.of(condition);
    }
  }

  public static List<Expression> splitDisjunctivePredicates(Expression condition) {
    if (JoinPredicatesHelper.isOr(condition)) {
      return Stream.concat(
              splitDisjunctivePredicates(((FunctionExpression) condition).getArguments().getFirst())
                  .stream(),
              splitDisjunctivePredicates(((FunctionExpression) condition).getArguments().getLast())
                  .stream())
          .collect(Collectors.toList());
    } else {
      return ImmutableList.of(condition);
    }
  }

  public static <L, R> Pair<List<L>, List<R>> unzip(List<Pair<L, R>> pairs) {
    List<L> leftList = new ArrayList<>();
    List<R> rightList = new ArrayList<>();
    for (Pair<L, R> pair : pairs) {
      leftList.add(pair.getLeft());
      rightList.add(pair.getRight());
    }
    return Pair.of(leftList, rightList);
  }

  public static boolean isAnd(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.AND);
  }

  public static boolean isOr(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.OR);
  }

  public static boolean isEqual(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.EQUAL);
  }

  public static boolean isNot(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.NOT);
  }

  public static boolean isXor(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.XOR);
  }

  public static boolean isNotEqual(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.NOTEQUAL);
  }

  public static boolean isLess(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.LESS);
  }

  public static boolean isLte(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.LTE);
  }

  public static boolean isGreater(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.GREATER);
  }

  public static boolean isGte(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.GTE);
  }

  public static boolean isLike(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.LIKE);
  }

  public static boolean isNotLike(Expression expression) {
    return instanceOf(expression, BuiltinFunctionName.NOT_LIKE);
  }
}
