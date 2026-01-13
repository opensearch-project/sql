/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.calcite.validate.ValidationUtils.createUDTWithAttributes;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
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

  /**
   * Creates a custom TypeCoercion instance for PPL. This can be used as a TypeCoercionFactory.
   *
   * @param typeFactory the type factory
   * @param validator the SQL validator
   * @return custom PplTypeCoercion instance
   */
  public static TypeCoercion create(RelDataTypeFactory typeFactory, SqlValidator validator) {
    return new PplTypeCoercion(typeFactory, validator);
  }

  public PplTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  @Override
  public @Nullable RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
    RelDataType casted = super.implicitCast(in, expected);
    if (casted == null) {
      // String -> DATETIME is converted to String -> TIMESTAMP
      if (OpenSearchTypeUtil.isCharacter(in) && expected == SqlTypeFamily.DATETIME) {
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
    if (OpenSearchTypeUtil.isUserDefinedType(toType) && OpenSearchTypeUtil.isCharacter(fromType)) {
      need = true;
    }
    return need;
  }

  @Override
  protected boolean dateTimeStringEquality(
      SqlCallBinding binding, RelDataType left, RelDataType right) {
    if (OpenSearchTypeUtil.isCharacter(left) && OpenSearchTypeUtil.isDatetime(right)) {
      // Use user-defined types in place of inbuilt datetime types
      RelDataType r =
          OpenSearchTypeUtil.isUserDefinedType(right)
              ? right
              : ValidationUtils.createUDTWithAttributes(factory, right, right.getSqlTypeName());
      return coerceOperandType(binding.getScope(), binding.getCall(), 0, r);
    }
    if (OpenSearchTypeUtil.isCharacter(right) && OpenSearchTypeUtil.isDatetime(left)) {
      RelDataType l =
          OpenSearchTypeUtil.isUserDefinedType(left)
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
    // - (ip, string) -> ip
    if (type1 != null && type2 != null) {
      boolean anyNullable = type1.isNullable() || type2.isNullable();
      if ((SqlTypeUtil.isDate(type1) && OpenSearchTypeUtil.isTime(type2))
          || (OpenSearchTypeUtil.isTime(type1) && SqlTypeUtil.isDate(type2))) {
        return factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.TIMESTAMP), anyNullable);
      }
      if (OpenSearchTypeUtil.isTime(type1) && SqlTypeUtil.isTimestamp(type2)) {
        return factory.createTypeWithNullability(type2, anyNullable);
      }
      if (SqlTypeUtil.isTimestamp(type1) && OpenSearchTypeUtil.isTime(type2)) {
        return factory.createTypeWithNullability(type1, anyNullable);
      }
      if (OpenSearchTypeUtil.isIp(type1) && OpenSearchTypeUtil.isCharacter(type2)) {
        return factory.createTypeWithNullability(type1, anyNullable);
      }
      if (OpenSearchTypeUtil.isCharacter(type1) && OpenSearchTypeUtil.isIp(type2)) {
        return factory.createTypeWithNullability(type2, anyNullable);
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
    if (!needToCast(scope, operand, targetType, PplTypeCoercionRule.lenientInstance())) {
      return false;
    }
    // Fix up nullable attr.
    RelDataType targetType1 = ValidationUtils.syncAttributes(factory, operandType, targetType);
    SqlNode desired = castTo(operand, targetType1);
    call.setOperand(index, desired);
    // SAFE_CAST always results in nullable return type. See
    // SqlCastFunction#createTypeWithNullabilityFromExpr
    if (SqlKind.SAFE_CAST.equals(desired.getKind())) {
      targetType1 = factory.createTypeWithNullability(targetType1, true);
    }
    updateInferredType(desired, targetType1);
    return true;
  }

  private static SqlNode castTo(SqlNode node, RelDataType type) {
    if (OpenSearchTypeUtil.isDatetime(type) || OpenSearchTypeUtil.isIp(type)) {
      ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(type);
      return switch (exprType) {
        case ExprCoreType.DATE ->
            PPLBuiltinOperators.DATE.createCall(node.getParserPosition(), node);
        case ExprCoreType.TIMESTAMP ->
            PPLBuiltinOperators.TIMESTAMP.createCall(node.getParserPosition(), node);
        case ExprCoreType.TIME ->
            PPLBuiltinOperators.TIME.createCall(node.getParserPosition(), node);
        case ExprCoreType.IP -> PPLBuiltinOperators.IP.createCall(node.getParserPosition(), node);
        default -> throw new UnsupportedOperationException("Unsupported type: " + exprType);
      };
    }
    // Use CAST when node is a literal AND not a string literal
    // Use SAFE_CAST in rest cases to avoid throwing errors when the source node is malformatted
    SqlOperator cast =
        (node.getKind() == SqlKind.LITERAL && !(node instanceof SqlCharStringLiteral))
            ? SqlStdOperatorTable.CAST
            : SqlLibraryOperators.SAFE_CAST;
    return cast.createCall(
        node.getParserPosition(),
        node,
        SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
  }
}
