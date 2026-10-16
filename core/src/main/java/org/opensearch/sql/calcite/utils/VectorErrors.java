/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * User errors for vector columns ({@link ExprVectorType}). Each factory method is named after an
 * error case of the PPL k-NN design and returns a {@link SemanticCheckException}, which the REST
 * layer reports as HTTP 400. It is marked {@link SemanticCheckException#withoutCalciteFallback()},
 * so the query never falls back to the V2 engine, even with {@code
 * plugins.calcite.fallback.allowed}.
 */
public final class VectorErrors {

  /** Message of the <i>vector not comparable</i> error. */
  public static final String VECTOR_NOT_COMPARABLE_MESSAGE =
      "VECTOR values cannot be compared, sorted or grouped";

  /**
   * Aggregates whose result depends on an order or on equality of their argument values: {@code
   * min} and {@code max} order them, {@code dc} / {@code distinct_count} and {@code values}
   * deduplicate them. Aggregates that only count or pick values ({@code count}, {@code first},
   * {@code last}, {@code earliest}, {@code latest}) accept vectors. {@code list} and {@code take}
   * reject any array argument, vectors included, through their own type checks, and numeric ones
   * ({@code avg}, {@code sum}, ...) are left to theirs.
   */
  private static final Set<BuiltinFunctionName> ORDER_OR_EQUALITY_AGGREGATES =
      Set.of(
          BuiltinFunctionName.MIN,
          BuiltinFunctionName.MAX,
          BuiltinFunctionName.DISTINCT_COUNT_APPROX,
          BuiltinFunctionName.VALUES);

  private VectorErrors() {}

  /** A vector error, which never makes the query fall back to the V2 engine. */
  private static SemanticCheckException error(String message) {
    return new SemanticCheckException(message).withoutCalciteFallback();
  }

  /**
   * <i>vector not comparable</i>: a vector column is used where an order or equality is needed, for
   * example in {@code =}, {@code <}, {@code sort}, {@code stats ... by} or {@code dedup}.
   */
  public static SemanticCheckException vectorNotComparable() {
    return error(VECTOR_NOT_COMPARABLE_MESSAGE);
  }

  /**
   * <i>vector not comparable</i> with a detail that names the column and how to avoid the error.
   *
   * @param detail appended to {@link #VECTOR_NOT_COMPARABLE_MESSAGE} after a colon
   */
  public static SemanticCheckException vectorNotComparable(String detail) {
    return error(VECTOR_NOT_COMPARABLE_MESSAGE + ": " + detail);
  }

  /**
   * <i>mixed vector types</i>: vector columns of different dimension or data type, or a vector and
   * a non-vector, are combined into one column, e.g. by {@code append}, {@code union} or a
   * conditional expression.
   *
   * @param first the vector type
   * @param other the type it cannot be combined with
   */
  public static SemanticCheckException mixedVectorTypes(RelDataType first, RelDataType other) {
    return error("cannot combine " + first + " and " + other);
  }

  /**
   * A base64 vector whose element encoding is not known: the indices of a multi-index query map the
   * field with float and byte data types, and the value's index is not known (e.g. in a script
   * pushed down to OpenSearch) or is not one of those whose mappings the query read. Decoding it
   * either way could give wrong numbers.
   *
   * @param field the field or column of the value
   * @param index the index of the value, or {@code null} if not known
   */
  public static SemanticCheckException undecodableVector(String field, @Nullable String index) {
    return error(
        index == null
            ? String.format(
                "cannot decode the base64 vector of [%s]: the indices of the query map it with"
                    + " float and byte data types, and the index of this value is not known",
                field)
            : String.format(
                "cannot decode the base64 vector of [%s] from index [%s]: the indices of the query"
                    + " map it with float and byte data types, and the mapping of index [%s] is"
                    + " not known",
                field, index, index));
  }

  /**
   * Fail with <i>vector not comparable</i> if the type is a vector.
   *
   * @param type the type of an operand, sort key, group-by key or dedup field
   */
  public static void requireComparable(RelDataType type) {
    if (OpenSearchTypeFactory.isVectorType(type)) {
      throw vectorNotComparable();
    }
  }

  /**
   * Fail with <i>vector not comparable</i> if an aggregate that orders or deduplicates its argument
   * (see {@link #ORDER_OR_EQUALITY_AGGREGATES}), or any {@code DISTINCT} aggregate such as {@code
   * count(distinct v)}, is applied to a vector.
   *
   * @param function the aggregate
   * @param distinct whether the aggregate is {@code DISTINCT}
   * @param field the aggregated expression, or {@code null} (e.g. {@code count()})
   */
  public static void requireComparableAggregate(
      BuiltinFunctionName function, boolean distinct, @Nullable RexNode field) {
    if (field != null && (distinct || ORDER_OR_EQUALITY_AGGREGATES.contains(function))) {
      requireComparable(field.getType());
    }
  }

  /**
   * Fail with <i>vector not comparable</i> if any node has a vector type.
   *
   * @param nodes operands, sort keys, group-by keys or dedup fields
   */
  public static void requireComparable(Iterable<? extends RexNode> nodes) {
    for (RexNode node : nodes) {
      requireComparable(node.getType());
    }
  }
}
