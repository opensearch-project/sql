/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MVDedup function that removes duplicate values from a multivalue array while preserving order.
 * Returns an array with duplicates removed or null for consistent type behavior.
 */
public class MVDedupFunctionImpl extends ImplementorUDF {

  public MVDedupFunctionImpl() {
    super(new MVDedupImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();

      if (sqlOperatorBinding.getOperandCount() == 0) {
        return typeFactory.createSqlType(SqlTypeName.NULL);
      }

      RelDataType operandType = sqlOperatorBinding.getOperandType(0);

      // If operand is already an array, return the same array type
      if (!operandType.isStruct() && operandType.getComponentType() != null) {
        return createArrayType(
            typeFactory,
            typeFactory.createTypeWithNullability(operandType.getComponentType(), true),
            true);
      }

      // If operand is not an array, wrap it in an array type
      return createArrayType(
          typeFactory, typeFactory.createTypeWithNullability(operandType, true), true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class MVDedupImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(MVDedupFunctionImpl.class, "mvdedup", Object.class),
          translatedOperands.get(0));
    }
  }

  public static Object mvdedup(Object array) {
    return MVDedupCore.removeDuplicates(array);
  }
}
