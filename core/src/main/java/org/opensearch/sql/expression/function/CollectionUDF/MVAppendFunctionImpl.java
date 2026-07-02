/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MVAppend function that appends all elements from arguments to create an array. Always returns an
 * array or null for consistent type behavior.
 */
public class MVAppendFunctionImpl extends ImplementorUDF {

  public MVAppendFunctionImpl() {
    super(new MVAppendImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();

      if (sqlOperatorBinding.getOperandCount() == 0) {
        return typeFactory.createSqlType(SqlTypeName.NULL);
      }

      RelDataType elementType = determineElementType(sqlOperatorBinding, typeFactory);
      return createArrayType(
          typeFactory, typeFactory.createTypeWithNullability(elementType, true), true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  private static RelDataType determineElementType(
      SqlOperatorBinding sqlOperatorBinding, RelDataTypeFactory typeFactory) {
    RelDataType mostGeneralType = null;

    for (int i = 0; i < sqlOperatorBinding.getOperandCount(); i++) {
      RelDataType operandType = getComponentType(sqlOperatorBinding.getOperandType(i));

      mostGeneralType = updateMostGeneralType(mostGeneralType, operandType, typeFactory);
    }

    return mostGeneralType != null ? mostGeneralType : typeFactory.createSqlType(SqlTypeName.NULL);
  }

  private static RelDataType getComponentType(RelDataType operandType) {
    if (!operandType.isStruct() && operandType.getComponentType() != null) {
      return operandType.getComponentType();
    }
    return operandType;
  }

  private static RelDataType updateMostGeneralType(
      RelDataType current, RelDataType candidate, RelDataTypeFactory typeFactory) {
    if (current == null) {
      return candidate;
    }
    if (current.equals(candidate)) {
      return current;
    }
    // Widen via Calcite's {@code leastRestrictive} — the same routine
    // {@code SqlLibraryOperators.ARRAY} uses for its return-type inference. For genuinely
    // incompatible operand types (INT + VARCHAR, …) it returns null; fall back to {@code ANY}
    // there to preserve the in-process Calcite engine's {@code Object[]} runtime semantics
    // that pre-existing tests rely on. Promote DECIMAL → DOUBLE on the way through: the row
    // codec on the analytics-engine route maps DECIMAL cells to {@code FloatingPoint(DOUBLE)}
    // anyway, and an explicit DECIMAL element type triggers Calcite's element coercion to
    // BigDecimal, which downstream Avatica array accessors and the JSON formatter render
    // inconsistently across paths.
    RelDataType least = typeFactory.leastRestrictive(java.util.List.of(current, candidate));
    if (least == null) {
      return typeFactory.createSqlType(SqlTypeName.ANY);
    }
    if (least.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
    }
    return least;
  }

  public static class MVAppendImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      // Pre-cast each scalar operand to the call's element Java class so the result list is
      // homogeneously typed. Avatica's {@code AbstractCursor.ArrayAccessor} dispatches the
      // per-element accessor by the declared SQL type — e.g. {@code DoubleAccessor.getDouble}
      // does {@code (Double) value} — and would throw a runtime ClassCastException on an
      // {@code Integer} cell when the call's element type widens to DOUBLE. Array operands
      // pass through; their element-type alignment is the planner's responsibility.
      RelDataType elementType = call.getType().getComponentType();
      Class<?> elementClass =
          elementType == null ? Object.class : boxedJavaClass(elementType.getSqlTypeName());
      List<Expression> coerced = new ArrayList<>(translatedOperands.size());
      for (int i = 0; i < translatedOperands.size(); i++) {
        Expression op = translatedOperands.get(i);
        RelDataType opType = call.getOperands().get(i).getType();
        if (opType.getComponentType() != null || elementClass == Object.class) {
          coerced.add(op);
        } else {
          coerced.add(EnumUtils.convert(op, elementClass));
        }
      }
      // Pass the target element SqlTypeName so the runtime can align the elements flattened out of
      // ARRAY operands. Calcite does not element-wise cast inside an array operand, so
      // `mvappend(array(int_col), int_col * 2)` — where operand widening makes the result element
      // type BIGINT while `array(int_col)` still yields Integer cells — would otherwise throw
      // `Integer cannot be cast to Long` when the array is materialized. Scalars are already
      // pre-cast above; the runtime coercion is a no-op for them.
      SqlTypeName targetType = elementType == null ? null : elementType.getSqlTypeName();
      return Expressions.call(
          Types.lookupMethod(
              MVAppendFunctionImpl.class, "mvappendTyped", SqlTypeName.class, Object[].class),
          Expressions.constant(targetType, SqlTypeName.class),
          Expressions.newArrayInit(Object.class, coerced));
    }
  }

  /** Codegen entry point: coerces flattened elements to {@code elementType}. */
  public static Object mvappendTyped(SqlTypeName elementType, Object... args) {
    return MVAppendCore.collectElements(elementType, args);
  }

  /** Untyped entry point used by unit tests; performs no element coercion. */
  public static Object mvappend(Object... args) {
    return MVAppendCore.collectElements(args);
  }

  private static Class<?> boxedJavaClass(SqlTypeName sqlType) {
    return switch (sqlType) {
      case BOOLEAN -> Boolean.class;
      case TINYINT -> Byte.class;
      case SMALLINT -> Short.class;
      case INTEGER -> Integer.class;
      case BIGINT -> Long.class;
      case FLOAT, REAL -> Float.class;
      case DOUBLE -> Double.class;
      case DECIMAL -> BigDecimal.class;
      case CHAR, VARCHAR -> String.class;
      default -> Object.class;
    };
  }
}
