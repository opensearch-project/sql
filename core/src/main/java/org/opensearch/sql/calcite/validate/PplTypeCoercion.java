/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.calcite.validate.ValidationUtils.createUDTWithAttributes;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

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
    if (casted == null) {
      // String -> DATETIME is converted to String -> TIMESTAMP
      if (OpenSearchTypeFactory.isCharacter(in) && expected == SqlTypeFamily.DATETIME) {
        return createUDTWithAttributes(factory, in, OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP);
      }
      return null;
    }
    return switch (casted.getSqlTypeName()) {
      case SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP, SqlTypeName.BINARY ->
          createUDTWithAttributes(factory, in, casted.getSqlTypeName());
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
    if (OpenSearchTypeFactory.isUserDefinedType(toType)
        && OpenSearchTypeFactory.isCharacter(fromType)) {
      need = true;
    }
    return need;
  }

  @Override
  protected boolean dateTimeStringEquality(
      SqlCallBinding binding, RelDataType left, RelDataType right) {
    if (OpenSearchTypeFactory.isCharacter(left) && OpenSearchTypeFactory.isDatetime(right)) {
      // Use user-defined types in place of inbuilt datetime types
      RelDataType r =
          OpenSearchTypeFactory.isUserDefinedType(right)
              ? right
              : ValidationUtils.createUDTWithAttributes(factory, right, right.getSqlTypeName());
      return coerceOperandType(binding.getScope(), binding.getCall(), 0, r);
    }
    if (OpenSearchTypeFactory.isCharacter(right) && OpenSearchTypeFactory.isDatetime(left)) {
      RelDataType l =
          OpenSearchTypeFactory.isUserDefinedType(left)
              ? left
              : ValidationUtils.createUDTWithAttributes(factory, left, left.getSqlTypeName());
      return coerceOperandType(binding.getScope(), binding.getCall(), 1, l);
    }
    return false;
  }

  @Override
  public @Nullable RelDataType commonTypeForBinaryComparison(
      @Nullable RelDataType type1, @Nullable RelDataType type2) {
    // Prepend following rules for datetime comparisons:
    // - (date, time) -> timestamp
    // - (time, timestamp) -> timestamp
    if (type1 != null & type2 != null) {
      boolean anyNullable = type1.isNullable() || type2.isNullable();
      if ((SqlTypeUtil.isDate(type1) && OpenSearchTypeFactory.isTime(type2))
          || (OpenSearchTypeFactory.isTime(type1) && SqlTypeUtil.isDate(type2))) {
        return factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.TIMESTAMP), anyNullable);
      }
      if (OpenSearchTypeFactory.isTime(type1) && SqlTypeUtil.isTimestamp(type2)) {
        return factory.createTypeWithNullability(type2, anyNullable);
      }
      if (SqlTypeUtil.isTimestamp(type1) && OpenSearchTypeFactory.isTime(type2)) {
        return factory.createTypeWithNullability(type1, anyNullable);
      }
    }
    return super.commonTypeForBinaryComparison(type1, type2);
  }

  /**
   * Cast operand at index {@code index} to target type. we do this base on the fact that validate
   * happens before type coercion.
   */
  protected boolean coerceOperandType(
      @Nullable SqlValidatorScope scope, SqlCall call, int index, RelDataType targetType) {
    // Transform the JavaType to SQL type because the SqlDataTypeSpec
    // does not support deriving JavaType yet.
    if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
      targetType = ((JavaTypeFactory) factory).toSql(targetType);
    }

    SqlNode operand = call.getOperandList().get(index);
    if (operand instanceof SqlDynamicParam) {
      // Do not support implicit type coercion for dynamic param.
      return false;
    }
    requireNonNull(scope, "scope");
    RelDataType operandType = validator.deriveType(scope, operand);
    if (coerceStringToArray(call, operand, index, operandType, targetType)) {
      return true;
    }

    // Check it early.
    if (!needToCast(scope, operand, targetType, SqlTypeCoercionRule.lenientInstance())) {
      return false;
    }
    // Fix up nullable attr.
    RelDataType targetType1 = ValidationUtils.syncAttributes(factory, operandType, targetType);
    SqlNode desired = castTo(operand, targetType1);
    call.setOperand(index, desired);
    updateInferredType(desired, targetType1);
    return true;
  }

  private static SqlNode castTo(SqlNode node, RelDataType type) {
    if (OpenSearchTypeFactory.isDatetime(type)) {
      ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(type);
      return switch (exprType) {
        case ExprCoreType.DATE ->
            PPLBuiltinOperators.DATE.createCall(node.getParserPosition(), node);
        case ExprCoreType.TIMESTAMP ->
            PPLBuiltinOperators.TIMESTAMP.createCall(node.getParserPosition(), node);
        case ExprCoreType.TIME ->
            PPLBuiltinOperators.TIME.createCall(node.getParserPosition(), node);
        default -> throw new UnsupportedOperationException("Unsupported type: " + exprType);
      };
    }
    return SqlStdOperatorTable.CAST.createCall(
        node.getParserPosition(),
        node,
        SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
  }
}
