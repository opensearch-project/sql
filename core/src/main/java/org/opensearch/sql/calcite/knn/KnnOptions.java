/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Literal;

/**
 * The validated options of a k-NN step. Phase 1 knows {@code k} (the number of nearest neighbours),
 * {@code ef_search} (HNSW) and {@code nprobes} (IVF); any other name is <i>unknown knn option</i>.
 * Option names are matched case-insensitively.
 *
 * @param k the explicit {@code k=}, in [1, {@link #MAX_K}], or {@code null}
 * @param efSearch {@code ef_search}, a positive integer, or {@code null}
 * @param nprobes {@code nprobes}, a positive integer, or {@code null}
 */
public record KnnOptions(
    @Nullable Integer k, @Nullable Integer efSearch, @Nullable Integer nprobes) {

  /** Largest k the k-NN plugin accepts ({@code KNNQueryBuilder.K_MAX}). */
  public static final int MAX_K = 10_000;

  public static final String K = "k";
  public static final String EF_SEARCH = "ef_search";
  public static final String NPROBES = "nprobes";

  /** No options. */
  public static final KnnOptions NONE = new KnnOptions(null, null, null);

  /**
   * Read and validate options.
   *
   * @param options {@code name=value} options as written in the query
   * @return the validated options
   * @throws org.opensearch.sql.exception.SemanticCheckException <i>unknown knn option</i>, <i>k out
   *     of range</i>, or an invalid value or duplicate of another option
   */
  public static KnnOptions parse(List<Argument> options) {
    Integer k = null;
    Integer efSearch = null;
    Integer nprobes = null;
    Set<String> seen = new HashSet<>();
    for (Argument option : options) {
      String name = option.getArgName().toLowerCase(Locale.ROOT);
      if (!seen.add(name)) {
        throw KnnErrors.duplicateOption(name);
      }
      Object value = valueOf(option.getValue());
      switch (name) {
        case K -> k = parseK(value);
        case EF_SEARCH -> efSearch = parsePositive(EF_SEARCH, value);
        case NPROBES -> nprobes = parsePositive(NPROBES, value);
        default -> throw KnnErrors.unknownOption(option.getArgName());
      }
    }
    return new KnnOptions(k, efSearch, nprobes);
  }

  /**
   * Read an explicit {@code k=} value.
   *
   * @param value the literal's value
   * @return k
   * @throws org.opensearch.sql.exception.SemanticCheckException <i>k out of range</i> if it is not
   *     an integer in [1, {@link #MAX_K}]
   */
  public static int parseK(@Nullable Object value) {
    Long k = asLong(value);
    if (k == null || k < 1 || k > MAX_K) {
      throw KnnErrors.kOutOfRange(value);
    }
    return k.intValue();
  }

  /**
   * Validate an explicit or derived k.
   *
   * @param k the k
   * @return whether k is in [1, {@link #MAX_K}]
   */
  public static boolean isValidK(long k) {
    return k >= 1 && k <= MAX_K;
  }

  private static int parsePositive(String name, @Nullable Object value) {
    Long n = asLong(value);
    if (n == null || n < 1 || n > Integer.MAX_VALUE) {
      throw KnnErrors.invalidOptionValue(name, value);
    }
    return n.intValue();
  }

  private static @Nullable Object valueOf(@Nullable Literal literal) {
    return literal == null ? null : literal.getValue();
  }

  /** The value of an integer literal, or {@code null} for anything else. */
  private static @Nullable Long asLong(@Nullable Object value) {
    return switch (value) {
      case Integer i -> i.longValue();
      case Long l -> l;
      case Short s -> s.longValue();
      case Byte b -> b.longValue();
      case null, default -> null;
    };
  }
}
