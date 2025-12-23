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
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
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
   * Create a PPL-specific TypeCoercion instance usable as a TypeCoercionFactory.
   *
   * @param typeFactory the RelDataTypeFactory used to create SQL types
   * @param validator the SqlValidator used by the coercion
   * @return the PPL TypeCoercion instance
   */
  public static TypeCoercion create(RelDataTypeFactory typeFactory, SqlValidator validator) {
    return new PplTypeCoercion(typeFactory, validator);
  }

  /**
   * Constructs a PplTypeCoercion using the given type factory and SQL validator.
   *
   * @param typeFactory factory used to create and manipulate SQL types
   * @param validator   SQL validator used for type resolution and scope information
   */
  public PplTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  /**
   * Determines the implicit cast target for the given input type toward the expected SQL type family,
   * applying PPL-specific user-defined-type (UDT) rules for datetime and binary targets.
   *
   * @param in       the input type to be cast
   * @param expected the expected SQL type family
   * @return the resulting RelDataType to use for implicit casting, or `null` if no implicit cast is available;
   *         if no standard implicit cast exists and `in` is a character type while `expected` is DATETIME,
   *         returns a timestamp UDT derived from `in`; if a standard implicit cast yields DATE, TIME,
   *         TIMESTAMP, or BINARY, returns a UDT with attributes based on that target type and the original input;
   *         otherwise returns the standard cast target type
   */
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
   * Determines whether the given node must be cast to the target type, with special handling for user-defined types.
   *
   * Forces a cast when the target type is a user-defined type and the node's inferred type is a character type;
   * in all other cases, returns the default decision.
   *
   * @param scope       validation scope for deriving the node's type
   * @param node        expression to check for casting necessity
   * @param toType      target type to which the node may be cast
   * @param mappingRule rule guiding type mapping decisions
   * @return `true` if the node must be cast to `toType`, `false` otherwise
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

  /**
   * Coerces a string operand to a datetime user-defined type when comparing string and datetime operands.
   *
   * @param binding binding providing the call and scope for coercion
   * @param left left operand type of the comparison
   * @param right right operand type of the comparison
   * @return `true` if the character operand was coerced to a datetime UDT, `false` otherwise
   */
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

  /**
   * Determine the common type used for binary comparisons between two operand types,
   * applying PPL-specific rules for datetime and IP/string combinations.
   *
   * <p>Special rules:
   * - date with time (in either order) -> TIMESTAMP
   * - time with timestamp -> TIMESTAMP (preserving the timestamp side)
   * - IP with character/string (in either order) -> IP
   *
   * Nullability of the returned type is set to "nullable" if either input type is nullable.
   *
   * @param type1 the left operand type, or {@code null} if unknown
   * @param type2 the right operand type, or {@code null} if unknown
   * @return the resolved common type for comparison according to the above rules,
   *         or the superclass's common type resolution when no rule applies
   */
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
   * Attempts to coerce the operand at the given index of the call to the specified target type and,
   * if coercion is applied, replaces the operand with the corresponding cast expression.
   *
   * @param scope the validator scope used to derive the operand's current type; must not be null
   * @param call  the SqlCall containing the operand to coerce
   * @param index the zero-based index of the operand within the call to coerce
   * @param targetType the desired target SQL type for the operand
   * @return `true` if the operand was coerced and replaced in the call, `false` otherwise
   *
   * <p>Notes:
   * - Dynamic parameters are not coerced; the method returns `false` for SqlDynamicParam operands.
   * - If no cast is necessary according to coercion rules, the method returns `false`.</p>
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

  /**
   * Create a SqlNode that casts the given expression to the specified target type.
   *
   * If the target type is a datetime or IP type, produces the corresponding PPL builtin operator
   * call (DATE, TIMESTAMP, TIME, or IP). For all other types, produces a SAFE_CAST node to the
   * specified type and preserves the target type's nullability to avoid exceptions on malformed
   * values.
   *
   * @param node the expression to cast
   * @param type the desired target relational type; datetime/IP types produce PPL operator calls
   * @return a SqlNode that performs the cast (PPL builtin call for datetime/IP, or SAFE_CAST otherwise)
   * @throws UnsupportedOperationException if the target datetime/IP ExprType is not supported
   */
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
    // Use SAFE_CAST instead of CAST to avoid throwing errors when numbers are malformatted
    return SqlLibraryOperators.SAFE_CAST.createCall(
        node.getParserPosition(),
        node,
        SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
  }
}