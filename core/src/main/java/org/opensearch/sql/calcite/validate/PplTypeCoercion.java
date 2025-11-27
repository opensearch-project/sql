/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

/**
 * Custom type coercion implementation for PPL that extends Calcite's default type coercion with
 * additional restrictions.
 *
 * <p>This class implements a blacklist approach to prevent certain implicit type conversions that
 * are not allowed in PPL semantics.
 */
public class PplTypeCoercion extends TypeCoercionImpl {
  // A blacklist of coercions that are not allowed in PPL.
  // key cannot be cast from values
  private static final Map<SqlTypeFamily, Set<SqlTypeFamily>> BLACKLISTED_COERCIONS;

  static {
    // Initialize the blacklist for coercions that are not allowed in PPL.
    BLACKLISTED_COERCIONS =
        Map.of(
            SqlTypeFamily.CHARACTER,
            Set.of(SqlTypeFamily.NUMERIC),
            SqlTypeFamily.STRING,
            Set.of(SqlTypeFamily.NUMERIC),
            SqlTypeFamily.NUMERIC,
            Set.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.STRING));
  }

  public PplTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  @Override
  public boolean builtinFunctionCoercion(
      SqlCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies) {
    assert binding.getOperandCount() == operandTypes.size();
    if (IntStream.range(0, operandTypes.size())
        .anyMatch(i -> isBlacklistedCoercion(operandTypes.get(i), expectedFamilies.get(i)))) {
      return false;
    }
    return super.builtinFunctionCoercion(binding, operandTypes, expectedFamilies);
  }

  /**
   * Checks if a type coercion is blacklisted based on PPL rules.
   *
   * @param operandType the actual type of the operand
   * @param expectedFamily the expected type family
   * @return true if the coercion is blacklisted, false otherwise
   */
  private boolean isBlacklistedCoercion(RelDataType operandType, SqlTypeFamily expectedFamily) {
    if (BLACKLISTED_COERCIONS.containsKey(expectedFamily)) {
      Set<SqlTypeFamily> blacklistedFamilies = BLACKLISTED_COERCIONS.get(expectedFamily);
      if (blacklistedFamilies.contains(operandType.getSqlTypeName().getFamily())) {
        return true;
      }
    }
    return false;
  }
}
