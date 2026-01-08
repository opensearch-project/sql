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

    if (!current.equals(candidate)) {
      return typeFactory.createSqlType(SqlTypeName.ANY);
    } else {
      return current;
    }
  }

  public static class MVAppendImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(MVAppendFunctionImpl.class, "mvappend", Object[].class),
          Expressions.newArrayInit(Object.class, translatedOperands));
    }
  }

  public static Object mvappend(Object... args) {
    return collectElements(args);
  }

  /**
   * Collect non-null elements from `args`. If an item is a list, it will collect non-null elements
   * of the list. See {@ref MVAppendFunctionImplTest} for detailed behavior.
   */
  public static List<Object> collectElements(Object... args) {
    List<Object> elements = AppendCore.collectElementsToList(args);
    return elements.isEmpty() ? null : elements;
  }
}
