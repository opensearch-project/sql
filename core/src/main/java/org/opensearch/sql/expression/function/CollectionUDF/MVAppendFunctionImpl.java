/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.util.ArrayList;
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
    super(new MVAppendImplementor(), NullPolicy.ANY);
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
    boolean hasStringType = false;
    boolean hasNumericType = false;

    for (int i = 0; i < sqlOperatorBinding.getOperandCount(); i++) {
      RelDataType operandType = getComponentType(sqlOperatorBinding.getOperandType(i));

      if (isStringType(operandType)) {
        hasStringType = true;
      } else if (isNumericType(operandType)) {
        hasNumericType = true;
      }

      mostGeneralType = updateMostGeneralType(mostGeneralType, operandType, typeFactory);
    }

    if (hasStringType && hasNumericType) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    return mostGeneralType != null
        ? mostGeneralType
        : typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  private static RelDataType getComponentType(RelDataType operandType) {
    if (!operandType.isStruct() && operandType.getComponentType() != null) {
      return operandType.getComponentType();
    }
    return operandType;
  }

  private static boolean isStringType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    return typeName == SqlTypeName.VARCHAR || typeName == SqlTypeName.CHAR;
  }

  private static boolean isNumericType(RelDataType type) {
    return SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName());
  }

  private static RelDataType updateMostGeneralType(
      RelDataType current, RelDataType candidate, RelDataTypeFactory typeFactory) {
    if (current == null) {
      return candidate;
    }

    RelDataType leastRestrictive = typeFactory.leastRestrictive(List.of(current, candidate));
    return leastRestrictive != null ? leastRestrictive : current;
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
    List<Object> elements = collectElements(args);
    return elements.isEmpty() ? null : elements;
  }

  private static List<Object> collectElements(Object... args) {
    List<Object> elements = new ArrayList<>();

    for (Object arg : args) {
      if (arg == null) {
        continue;
      }

      if (arg instanceof List) {
        addListElements((List<?>) arg, elements);
      } else if (arg.getClass().isArray()) {
        addArrayElements((Object[]) arg, elements);
      } else {
        elements.add(arg);
      }
    }

    return elements;
  }

  private static void addListElements(List<?> list, List<Object> elements) {
    for (Object item : list) {
      if (item != null) {
        elements.add(item);
      }
    }
  }

  private static void addArrayElements(Object[] array, List<Object> elements) {
    for (Object item : array) {
      if (item != null) {
        elements.add(item);
      }
    }
  }
}
