/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.calcite.sql.type.SqlTypeAssignmentRule;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Provides PPL-specific type coercion rules that extend Calcite's default type coercion behavior.
 *
 * <p>This class defines additional type mapping rules for PPL, particularly for handling custom
 * types like IP addresses and number-to-string coercion. These additional rules are merged with
 * Calcite's built-in type coercion rules.
 *
 * <p>The additional mappings defined include:
 *
 * <ul>
 *   <li>IP can be coerced to/from string types
 *   <li>VARCHAR can be coerced from numeric types
 * </ul>
 *
 * <p>Three variants of type coercion rules are provided:
 *
 * <ul>
 *   <li>{@link #instance()} - Standard type coercion rules
 *   <li>{@link #lenientInstance()} - More permissive type coercion rules
 *   <li>{@link #assignmentInstance()} - Rules for type assignment validation
 * </ul>
 *
 * @see SqlTypeCoercionRule
 * @see PplTypeCoercion
 */
public class PplTypeCoercionRule {
  /**
   * PPL-specific additional type mapping rules
   *
   * <ul>
   *   <li>IP -> IP
   *   <li>CHARACTER -> IP
   *   <li>IP -> CHARACTER
   *   <li>NUMBER -> VARCHAR
   * </ul>
   */
  private static final Map<SqlTypeName, ImmutableSet<@NonNull SqlTypeName>> additionalMapping =
      Map.of(
          SqlTypeName.OTHER,
          ImmutableSet.of(SqlTypeName.OTHER, SqlTypeName.VARCHAR, SqlTypeName.CHAR),
          SqlTypeName.VARCHAR,
          ImmutableSet.<SqlTypeName>builder()
              .add(SqlTypeName.OTHER)
              .addAll(SqlTypeName.NUMERIC_TYPES)
              .build(),
          SqlTypeName.CHAR,
          ImmutableSet.of(SqlTypeName.OTHER));

  private static final SqlTypeCoercionRule INSTANCE =
      SqlTypeCoercionRule.instance(
          mergeMapping(SqlTypeCoercionRule.instance().getTypeMapping(), additionalMapping));
  private static final SqlTypeCoercionRule LENIENT_INSTANCE =
      SqlTypeCoercionRule.instance(
          mergeMapping(SqlTypeCoercionRule.lenientInstance().getTypeMapping(), additionalMapping));
  private static final SqlTypeCoercionRule ASSIGNMENT_INSTANCE =
      SqlTypeCoercionRule.instance(
          mergeMapping(SqlTypeAssignmentRule.instance().getTypeMapping(), additionalMapping));

  public static SqlTypeCoercionRule instance() {
    return INSTANCE;
  }

  /** Returns an instance that allows more lenient type coercion. */
  public static SqlTypeCoercionRule lenientInstance() {
    return LENIENT_INSTANCE;
  }

  /** Rules that determine whether a type is assignable from another type. */
  public static SqlTypeCoercionRule assignmentInstance() {
    return ASSIGNMENT_INSTANCE;
  }

  private static <T> Map<T, ImmutableSet<@NonNull T>> mergeMapping(
      Map<T, ImmutableSet<@NonNull T>> base, Map<T, ImmutableSet<@NonNull T>> addition) {
    return Stream.concat(base.entrySet().stream(), addition.entrySet().stream())
        .collect(
            Collectors.collectingAndThen(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (b, a) -> {
                      Set<T> combined = new HashSet<>(b);
                      combined.addAll(a);
                      return ImmutableSet.copyOf(combined);
                    }),
                ImmutableMap::copyOf));
  }
}
