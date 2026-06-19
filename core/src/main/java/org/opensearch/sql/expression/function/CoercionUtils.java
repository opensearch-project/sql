/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.calcite.type.ExprBinaryType;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.exception.ExpressionEvaluationException;

/**
 * RelDataType-native widening, casting, and common-type resolution for PPL function dispatch.
 *
 * <p>This module models a small widening DAG over a tag space derived from {@link RelDataType}:
 *
 * <ul>
 *   <li>numeric SQL types (BYTE → SHORT → INTEGER → LONG → FLOAT → DOUBLE)
 *   <li>STRING (VARCHAR/CHAR)
 *   <li>BOOLEAN
 *   <li>OpenSearch UDTs (DATE, TIME, TIMESTAMP, IP, BINARY) detected via {@code instanceof}
 * </ul>
 *
 * <p>The widening rules mirror prior behavior: {@code STRING + NUMERIC → DOUBLE}, {@code DATE +
 * TIME → TIMESTAMP}, {@code STRING + BINARY → BINARY}; STRING widens to BOOLEAN/DATE/TIME/
 * TIMESTAMP/IP; numerics widen along the integer/float chain; etc.
 */
public final class CoercionUtils {
  private CoercionUtils() {}

  /**
   * Casts the arguments to one of the typeChecker's allowed signatures. Returns null if no
   * combination of parameter types matches the arguments or if casting fails.
   */
  public static @Nullable List<RexNode> castArguments(
      RexBuilder builder, PPLTypeChecker typeChecker, List<RexNode> arguments) {
    List<List<RelDataType>> paramTypeCombinations = typeChecker.getParameterTypes();

    List<RelDataType> sourceTypes = arguments.stream().map(RexNode::getType).toList();
    // Candidate parameter signatures ordered by decreasing widening distance
    PriorityQueue<Pair<List<RelDataType>, Integer>> rankedSignatures =
        new PriorityQueue<>((left, right) -> Integer.compare(right.getValue(), left.getValue()));
    for (List<RelDataType> paramTypes : paramTypeCombinations) {
      int distance = signatureDistance(sourceTypes, paramTypes);
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
   * Widen the arguments to the widest type found among them. If no widest type can be determined,
   * returns null.
   */
  public static @Nullable List<RexNode> widenArguments(
      RexBuilder builder, List<RexNode> arguments) {
    RelDataType widestType = findWidestType(arguments);
    if (widestType == null) {
      return null;
    }
    return arguments.stream().map(arg -> cast(builder, widestType, arg)).toList();
  }

  /** Returns true if any of the operands is a string-like (VARCHAR/CHAR) RexNode. */
  public static boolean hasString(List<RexNode> rexNodeList) {
    return rexNodeList.stream().map(RexNode::getType).anyMatch(SqlTypeUtil::isCharacter);
  }

  /**
   * Resolves the widening common type for two {@link RelDataType}s using PPL coercion rules. Used
   * primarily by tests to verify the rule set.
   */
  @VisibleForTesting
  public static Optional<RelDataType> resolveCommonType(RelDataType left, RelDataType right) {
    return COMMON_COERCION_RULES.stream()
        .map(rule -> rule.apply(left, right))
        .flatMap(Optional::stream)
        .findFirst();
  }

  // -------- Internals --------

  private static @Nullable List<RexNode> castArguments(
      RexBuilder builder, List<RelDataType> paramTypes, List<RexNode> arguments) {
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

  private static @Nullable RexNode cast(RexBuilder builder, RelDataType targetType, RexNode arg) {
    RelDataType argType = arg.getType();
    if (!shouldCast(argType, targetType)) {
      return arg;
    }
    if (distance(argType, targetType) != IMPOSSIBLE_WIDENING) {
      return builder.makeCast(targetType, arg, true, true);
    }
    return resolveCommonType(argType, targetType)
        .map(common -> builder.makeCast(common, arg, true, true))
        .orElse(null);
  }

  private static @Nullable RelDataType findWidestType(List<RexNode> arguments) {
    if (arguments.isEmpty()) {
      return null;
    }
    RelDataType widest = arguments.getFirst().getType();
    if (arguments.size() == 1) {
      return widest;
    }
    for (int i = 1; i < arguments.size(); i++) {
      RelDataType type = arguments.get(i).getType();
      try {
        final RelDataType current = widest;
        widest = resolveCommonType(widest, type).orElseGet(() -> max(current, type));
      } catch (ExpressionEvaluationException e) {
        return null;
      }
    }
    return widest;
  }

  // -------- Tag space + widening DAG --------

  /**
   * A small enum that tags every {@link RelDataType} the coercion rules need to reason about. Using
   * an enum means the widening DAG is closed and self-contained.
   */
  private enum CoercionTag {
    UNDEFINED, // Calcite NULL — compatible with anything
    UNKNOWN, // Anything we can't classify
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    BOOLEAN,
    DATE,
    TIME,
    TIMESTAMP,
    IP,
    BINARY,
    INTERVAL,
    ARRAY,
    STRUCT,
    GEOMETRY;
  }

  private static CoercionTag tagOf(RelDataType type) {
    if (type instanceof ExprDateType) return CoercionTag.DATE;
    if (type instanceof ExprTimeType) return CoercionTag.TIME;
    if (type instanceof ExprTimeStampType) return CoercionTag.TIMESTAMP;
    if (type instanceof ExprIPType) return CoercionTag.IP;
    if (type instanceof ExprBinaryType) return CoercionTag.BINARY;
    SqlTypeName name = type.getSqlTypeName();
    if (name == null) return CoercionTag.UNKNOWN;
    return switch (name) {
      case TINYINT -> CoercionTag.BYTE;
      case SMALLINT -> CoercionTag.SHORT;
      case INTEGER -> CoercionTag.INTEGER;
      case BIGINT -> CoercionTag.LONG;
      case REAL, FLOAT -> CoercionTag.FLOAT;
      case DOUBLE, DECIMAL -> CoercionTag.DOUBLE;
      case CHAR, VARCHAR -> CoercionTag.STRING;
      case BOOLEAN -> CoercionTag.BOOLEAN;
      case BINARY, VARBINARY -> CoercionTag.BINARY;
      case DATE -> CoercionTag.DATE;
      case TIME, TIME_WITH_LOCAL_TIME_ZONE, TIME_TZ -> CoercionTag.TIME;
      case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_TZ -> CoercionTag.TIMESTAMP;
      case NULL -> CoercionTag.UNDEFINED;
      case ARRAY, MULTISET -> CoercionTag.ARRAY;
      case MAP, ROW, STRUCTURED -> CoercionTag.STRUCT;
      case GEOMETRY -> CoercionTag.GEOMETRY;
      default -> {
        if (SqlTypeName.INTERVAL_TYPES.contains(name)) {
          yield CoercionTag.INTERVAL;
        }
        yield CoercionTag.UNKNOWN;
      }
    };
  }

  /**
   * Parents in the widening DAG. Mirrors {@code ExprCoreType} parent links. A type widens to its
   * parent and (transitively) to grandparents.
   */
  private static final Map<CoercionTag, List<CoercionTag>> PARENTS = buildParents();

  private static Map<CoercionTag, List<CoercionTag>> buildParents() {
    Map<CoercionTag, List<CoercionTag>> p = new HashMap<>();
    p.put(CoercionTag.BYTE, List.of(CoercionTag.SHORT));
    p.put(CoercionTag.SHORT, List.of(CoercionTag.INTEGER));
    p.put(CoercionTag.INTEGER, List.of(CoercionTag.LONG));
    p.put(CoercionTag.LONG, List.of(CoercionTag.FLOAT));
    p.put(CoercionTag.FLOAT, List.of(CoercionTag.DOUBLE));
    // STRING has TIMESTAMP as a direct parent (in addition to DATE/TIME/BOOLEAN/IP) so the
    // STRING→TIMESTAMP widening cost matches STRING→DOUBLE (1). This preserves baseline ranking
    // when both NUMERIC and TIMESTAMP signatures are candidates for a STRING input.
    p.put(
        CoercionTag.STRING,
        List.of(
            CoercionTag.BOOLEAN,
            CoercionTag.DATE,
            CoercionTag.TIME,
            CoercionTag.TIMESTAMP,
            CoercionTag.IP));
    p.put(CoercionTag.DATE, List.of(CoercionTag.TIMESTAMP));
    p.put(CoercionTag.TIME, List.of(CoercionTag.TIMESTAMP));
    return p;
  }

  private static List<CoercionTag> parentsOf(CoercionTag tag) {
    return PARENTS.getOrDefault(tag, List.of());
  }

  private static final Set<CoercionTag> NUMBER_TAGS =
      Set.of(
          CoercionTag.BYTE,
          CoercionTag.SHORT,
          CoercionTag.INTEGER,
          CoercionTag.LONG,
          CoercionTag.FLOAT,
          CoercionTag.DOUBLE);

  // -------- Distance and "shouldCast" --------

  private static final int IMPOSSIBLE_WIDENING = Integer.MAX_VALUE;
  private static final int TYPE_EQUAL = 0;

  private static int distance(RelDataType from, RelDataType to) {
    return distance(tagOf(from), tagOf(to), TYPE_EQUAL);
  }

  private static int distance(CoercionTag from, CoercionTag to) {
    return distance(from, to, TYPE_EQUAL);
  }

  private static int distance(CoercionTag from, CoercionTag to, int acc) {
    if (from == to) return acc;
    if (from == CoercionTag.UNKNOWN) return IMPOSSIBLE_WIDENING;
    // UNDEFINED (NULL literal) widens to any concrete type at unit cost — mirrors v2 where every
    // concrete ExprCoreType has UNDEFINED as a parent.
    if (from == CoercionTag.UNDEFINED && to != CoercionTag.UNKNOWN) return acc + 1;
    // STRING widens directly to DOUBLE (custom rule mirroring v2 behavior).
    if (from == CoercionTag.STRING && to == CoercionTag.DOUBLE) return acc + 1;
    List<CoercionTag> parents = parentsOf(from);
    if (parents.isEmpty()) return IMPOSSIBLE_WIDENING;
    int best = IMPOSSIBLE_WIDENING;
    for (CoercionTag parent : parents) {
      int d = distance(parent, to, acc + 1);
      if (d < best) best = d;
    }
    return best;
  }

  /** The argument should be cast to the target type when their CoercionTags differ. */
  private static boolean shouldCast(RelDataType source, RelDataType target) {
    return tagOf(source) != tagOf(target);
  }

  /**
   * The widest of two types: the one that the other can widen to. Throws if neither widens to the
   * other.
   */
  static RelDataType max(RelDataType type1, RelDataType type2) {
    int t1To2 = distance(type1, type2);
    int t2To1 = distance(type2, type1);
    if (t1To2 == IMPOSSIBLE_WIDENING && t2To1 == IMPOSSIBLE_WIDENING) {
      throw new ExpressionEvaluationException(
          String.format("no max type of %s and %s ", type1, type2));
    }
    return t1To2 == IMPOSSIBLE_WIDENING ? type1 : type2;
  }

  private static int signatureDistance(
      List<RelDataType> sourceTypes, List<RelDataType> targetTypes) {
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

  // -------- Common-type rules --------

  private static boolean hasString(RelDataType l, RelDataType r) {
    return tagOf(l) == CoercionTag.STRING || tagOf(r) == CoercionTag.STRING;
  }

  private static boolean hasNumber(RelDataType l, RelDataType r) {
    return NUMBER_TAGS.contains(tagOf(l)) || NUMBER_TAGS.contains(tagOf(r));
  }

  private static boolean hasBinary(RelDataType l, RelDataType r) {
    return tagOf(l) == CoercionTag.BINARY || tagOf(r) == CoercionTag.BINARY;
  }

  private static boolean areDateAndTime(RelDataType l, RelDataType r) {
    CoercionTag lt = tagOf(l);
    CoercionTag rt = tagOf(r);
    return (lt == CoercionTag.DATE && rt == CoercionTag.TIME)
        || (lt == CoercionTag.TIME && rt == CoercionTag.DATE);
  }

  private static final List<CoercionRule> COMMON_COERCION_RULES =
      List.of(
          CoercionRule.of(
              CoercionUtils::areDateAndTime,
              (l, r) -> OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, 9)),
          CoercionRule.of(
              (l, r) -> hasString(l, r) && hasNumber(l, r),
              (l, r) -> OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE)),
          // (BINARY, STRING) → BINARY: ip/binary columns compared with string literals.
          // ExtendedRexBuilder.makeCast wraps the literal into VARBINARY when it sees this.
          CoercionRule.of(
              (l, r) -> hasString(l, r) && hasBinary(l, r),
              (l, r) -> OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY)));

  private record CoercionRule(
      BiPredicate<RelDataType, RelDataType> predicate, BinaryOperator<RelDataType> resolver) {

    Optional<RelDataType> apply(RelDataType left, RelDataType right) {
      return predicate.test(left, right)
          ? Optional.of(resolver.apply(left, right))
          : Optional.empty();
    }

    static CoercionRule of(
        BiPredicate<RelDataType, RelDataType> predicate, BinaryOperator<RelDataType> resolver) {
      return new CoercionRule(predicate, resolver);
    }
  }
}
