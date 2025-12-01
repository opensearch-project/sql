/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.opensearch.sql.calcite.validate.ValidationUtils.createUDTWithAttributes;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

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
    BLACKLISTED_COERCIONS = Map.of();
    //        Map.of(
    //            SqlTypeFamily.CHARACTER,
    //            Set.of(SqlTypeFamily.NUMERIC),
    //            SqlTypeFamily.STRING,
    //            Set.of(SqlTypeFamily.NUMERIC),
    //            SqlTypeFamily.NUMERIC,
    //            Set.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.STRING));
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

  @Override
  public @Nullable RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
    RelDataType casted = super.implicitCast(in, expected);
    return switch (casted.getSqlTypeName()) {
      case SqlTypeName.DATE ->
          createUDTWithAttributes(factory, in, OpenSearchTypeFactory.ExprUDT.EXPR_DATE);
      case SqlTypeName.TIME ->
          createUDTWithAttributes(factory, in, OpenSearchTypeFactory.ExprUDT.EXPR_TIME);
      case SqlTypeName.TIMESTAMP ->
          createUDTWithAttributes(factory, in, OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP);
      default -> casted;
    };
  }

  /**
   * Override super implementation to add special handling for user-defined types (UDTs). Otherwise,
   * UDTs will be regarded as character types, invalidating string->datetime casts.
   */
  @Override
  protected boolean needToCast(
      SqlValidatorScope scope, SqlNode node, RelDataType toType, SqlTypeMappingRule mappingRule) {
    boolean need = super.needToCast(scope, node, toType, mappingRule);
    RelDataType fromType = validator.deriveType(scope, node);
    if (OpenSearchTypeFactory.isUserDefinedType(toType) && SqlTypeUtil.isCharacter(fromType)) {
      need = true;
    }
    return need;
  }
}
