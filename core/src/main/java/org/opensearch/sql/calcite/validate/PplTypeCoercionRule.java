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

  /**
   * Provides the standard PPL type coercion rule merging Calcite's default mappings with PPL-specific mappings.
   *
   * @return the standard SqlTypeCoercionRule augmented with PPL-specific type mappings
   */
  public static SqlTypeCoercionRule instance() {
    return INSTANCE;
  }

  /**
   * Provides the PPL type coercion rule configured for lenient coercions.
   *
   * @return the lenient PPL {@link SqlTypeCoercionRule} that merges Calcite's lenient mappings with PPL-specific additional mappings
   */
  public static SqlTypeCoercionRule lenientInstance() {
    return LENIENT_INSTANCE;
  }

  /**
   * Provides the PPL-specific type assignment validation rule.
   *
   * @return the SqlTypeCoercionRule that validates whether a type can be assigned from another type
   */
  public static SqlTypeCoercionRule assignmentInstance() {
    return ASSIGNMENT_INSTANCE;
  }

  /**
   * Merges two mappings from keys to immutable sets, unioning value-sets for duplicate keys.
   *
   * Combines entries from `base` and `addition`; when a key exists in both maps, the resulting
   * value is the union of both sets. The returned map and all value sets are immutable.
   *
   * @param <T> the key and value element type
   * @param base the primary mapping to include in the result
   * @param addition the secondary mapping whose entries are merged into `base`
   * @return an immutable map containing all keys from both inputs with immutable sets of merged values
   */
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