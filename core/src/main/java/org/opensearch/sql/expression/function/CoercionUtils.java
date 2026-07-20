/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;

/**
 * PPL function-dispatch widening. Signatures expose {@link RelDataType} at the public boundary, but
 * the widening lattice and common-type rules operate on {@link ExprType} — the source of truth for
 * OpenSearch's type hierarchy. RelDataType inputs are converted at the entry via {@link
 * OpenSearchTypeFactory#convertRelDataTypeToExprType}, and results are round-tripped back with
 * {@link OpenSearchTypeFactory#convertExprTypeToRelDataType} — which returns the UDT variant for
 * temporal / IP types so downstream casts route through the UDT path automatically.
 */
public final class CoercionUtils {
  private CoercionUtils() {}

  /**
   * Casts the arguments to one of the typeChecker's allowed signatures. Returns null if no
   * combination of parameter types matches the arguments or if casting fails.
   */
  public static @Nullable List<RexNode> castArguments(
      RexBuilder builder, PPLTypeChecker typeChecker, List<RexNode> arguments) {
    List<List<ExprType>> paramTypeCombinations =
        typeChecker.getParameterTypes().stream().map(CoercionUtils::toExprTypes).toList();

    List<ExprType> sourceTypes = arguments.stream().map(CoercionUtils::exprTypeOf).toList();
    PriorityQueue<Pair<List<ExprType>, Integer>> rankedSignatures =
        new PriorityQueue<>((left, right) -> Integer.compare(right.getValue(), left.getValue()));
    for (List<ExprType> paramTypes : paramTypeCombinations) {
      int distance = distance(sourceTypes, paramTypes);
      if (distance == TYPE_EQUAL) {
        return castArguments(builder, paramTypes, arguments);
      }
      Optional.of(distance)
          .filter(value -> value != IMPOSSIBLE_WIDENING)
          .ifPresent(value -> rankedSignatures.add(Pair.of(paramTypes, value)));
    }
    return Optional.ofNullable(rankedSignatures.peek())
        .map(Pair::getKey)
        .map(paramTypes -> castArguments(builder, paramTypes, arguments))
        .orElse(null);
  }

  /**
   * Widen the arguments to the widest type found among them. Returns null if no widest type can be
   * determined.
   */
  public static @Nullable List<RexNode> widenArguments(
      RexBuilder builder, List<RexNode> arguments) {
    ExprType widestType = findWidestType(arguments);
    if (widestType == null) {
      return null;
    }
    return arguments.stream().map(arg -> cast(builder, widestType, arg)).toList();
  }

  /** Returns true if any of the operands has {@link ExprCoreType#STRING} type. */
  public static boolean hasString(List<RexNode> rexNodeList) {
    return rexNodeList.stream()
        .map(CoercionUtils::exprTypeOf)
        .anyMatch(t -> t == ExprCoreType.STRING);
  }

  /**
   * Resolves the widening common type for two {@link RelDataType}s using PPL coercion rules. Used
   * primarily by tests to verify the rule set.
   */
  @VisibleForTesting
  public static Optional<RelDataType> resolveCommonType(RelDataType left, RelDataType right) {
    return resolveCommonType(
            OpenSearchTypeFactory.convertRelDataTypeToExprType(left),
            OpenSearchTypeFactory.convertRelDataTypeToExprType(right))
        .map(OpenSearchTypeFactory::convertExprTypeToRelDataType);
  }

  // -------- Internals --------

  private static ExprType exprTypeOf(RexNode node) {
    return OpenSearchTypeFactory.convertRelDataTypeToExprType(node.getType());
  }

  private static List<ExprType> toExprTypes(List<RelDataType> types) {
    return types.stream().map(OpenSearchTypeFactory::convertRelDataTypeToExprType).toList();
  }

  private static @Nullable List<RexNode> castArguments(
      RexBuilder builder, List<ExprType> paramTypes, List<RexNode> arguments) {
    if (paramTypes.size() != arguments.size()) {
      return null;
    }
    List<RexNode> castedArguments = new ArrayList<>();
    for (int i = 0; i < paramTypes.size(); i++) {
      RexNode castedArg = cast(builder, paramTypes.get(i), arguments.get(i));
      if (castedArg == null) {
        return null;
      }
      castedArguments.add(castedArg);
    }
    return castedArguments;
  }

  private static @Nullable RexNode cast(RexBuilder builder, ExprType targetType, RexNode arg) {
    ExprType argType = exprTypeOf(arg);
    if (!argType.shouldCast(targetType)) {
      return arg;
    }
    // Always make the cast target nullable. Safe cast returns null on parse failure or on a null
    // source, so a non-nullable declared target defeats null-propagation and leads to primitive
    // unboxing NPEs in downstream aggregations.
    if (distance(argType, targetType) != IMPOSSIBLE_WIDENING) {
      return builder.makeCast(
          OpenSearchTypeFactory.convertExprTypeToRelDataType(targetType, true), arg, true, true);
    }
    return resolveCommonType(argType, targetType)
        .map(
            common ->
                builder.makeCast(
                    OpenSearchTypeFactory.convertExprTypeToRelDataType(common, true),
                    arg,
                    true,
                    true))
        .orElse(null);
  }

  private static @Nullable ExprType findWidestType(List<RexNode> arguments) {
    if (arguments.isEmpty()) {
      return null;
    }
    ExprType widest = exprTypeOf(arguments.getFirst());
    if (arguments.size() == 1) {
      return widest;
    }
    for (int i = 1; i < arguments.size(); i++) {
      ExprType type = exprTypeOf(arguments.get(i));
      try {
        final ExprType current = widest;
        widest = resolveCommonType(widest, type).orElseGet(() -> max(current, type));
      } catch (ExpressionEvaluationException e) {
        return null;
      }
    }
    return widest;
  }

  // -------- Common-type rules --------

  private static Optional<ExprType> resolveCommonType(ExprType left, ExprType right) {
    return COMMON_COERCION_RULES.stream()
        .map(rule -> rule.apply(left, right))
        .flatMap(Optional::stream)
        .findFirst();
  }

  private static boolean areDateAndTime(ExprType left, ExprType right) {
    return (left == ExprCoreType.DATE && right == ExprCoreType.TIME)
        || (left == ExprCoreType.TIME && right == ExprCoreType.DATE);
  }

  private static boolean hasString(ExprType left, ExprType right) {
    return left == ExprCoreType.STRING || right == ExprCoreType.STRING;
  }

  private static boolean hasNumber(ExprType left, ExprType right) {
    return NUMBER_TYPES.contains(left) || NUMBER_TYPES.contains(right);
  }

  private static boolean hasBinary(ExprType left, ExprType right) {
    return left == ExprCoreType.BINARY || right == ExprCoreType.BINARY;
  }

  private static final Set<ExprType> NUMBER_TYPES = ExprCoreType.numberTypes();

  private static final List<CoercionRule> COMMON_COERCION_RULES =
      List.of(
          CoercionRule.of(CoercionUtils::areDateAndTime, (l, r) -> ExprCoreType.TIMESTAMP),
          CoercionRule.of(
              (l, r) -> hasString(l, r) && hasNumber(l, r), (l, r) -> ExprCoreType.DOUBLE),
          // (BINARY, STRING) → BINARY: ip/binary columns compared with string literals.
          // ExtendedRexBuilder.makeCast wraps the literal into VARBINARY when it sees this.
          CoercionRule.of(
              (l, r) -> hasString(l, r) && hasBinary(l, r), (l, r) -> ExprCoreType.BINARY));

  private record CoercionRule(
      BiPredicate<ExprType, ExprType> predicate, BinaryOperator<ExprType> resolver) {

    Optional<ExprType> apply(ExprType left, ExprType right) {
      return predicate.test(left, right)
          ? Optional.of(resolver.apply(left, right))
          : Optional.empty();
    }

    static CoercionRule of(
        BiPredicate<ExprType, ExprType> predicate, BinaryOperator<ExprType> resolver) {
      return new CoercionRule(predicate, resolver);
    }
  }

  // -------- Widening distance --------

  private static final int IMPOSSIBLE_WIDENING = Integer.MAX_VALUE;
  private static final int TYPE_EQUAL = 0;

  private static int distance(ExprType type1, ExprType type2) {
    return distance(type1, type2, TYPE_EQUAL);
  }

  private static int distance(ExprType type1, ExprType type2, int distance) {
    if (type1 == type2) {
      return distance;
    } else if (type1 == UNKNOWN) {
      return IMPOSSIBLE_WIDENING;
    } else if (type1 == ExprCoreType.STRING && type2 == ExprCoreType.DOUBLE) {
      return distance + 1;
    } else {
      return type1.getParent().stream()
          .map(parentOfType1 -> distance(parentOfType1, type2, distance + 1))
          .reduce(Math::min)
          .orElse(IMPOSSIBLE_WIDENING);
    }
  }

  private static int distance(List<ExprType> sourceTypes, List<ExprType> targetTypes) {
    if (sourceTypes.size() != targetTypes.size()) {
      return IMPOSSIBLE_WIDENING;
    }
    int total = 0;
    for (int i = 0; i < sourceTypes.size(); i++) {
      int d = distance(sourceTypes.get(i), targetTypes.get(i));
      if (d == IMPOSSIBLE_WIDENING) {
        return IMPOSSIBLE_WIDENING;
      }
      total += d;
    }
    return total;
  }

  /**
   * The widest of two types: the one that the other can widen to. Throws if neither widens to the
   * other.
   */
  static ExprType max(ExprType type1, ExprType type2) {
    int t1To2 = distance(type1, type2);
    int t2To1 = distance(type2, type1);
    if (t1To2 == IMPOSSIBLE_WIDENING && t2To1 == IMPOSSIBLE_WIDENING) {
      throw new ExpressionEvaluationException(
          String.format("no max type of %s and %s ", type1, type2));
    }
    return t1To2 == IMPOSSIBLE_WIDENING ? type1 : type2;
  }
}
