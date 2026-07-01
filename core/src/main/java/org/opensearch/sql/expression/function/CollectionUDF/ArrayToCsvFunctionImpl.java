/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** ARRAY_TO_CSV function implementation that converts an array to a CSV string. */
public class ArrayToCsvFunctionImpl extends ImplementorUDF {

  public ArrayToCsvFunctionImpl() {
    super(new ArrayToCsvImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // Accept ARRAY as first argument, optional STRING as second argument (delimiter)
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.ARRAY)
                .or(OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER)));
  }

  public static class ArrayToCsvImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      // Handle both 1-argument (with default delimiter) and 2-argument cases
      if (translatedOperands.size() == 1) {
        // ARRAY_TO_CSV(array) - use default delimiter ","
        return Expressions.call(
            Types.lookupMethod(
                ArrayToCsvFunctionImpl.class, "arrayToCsv", List.class, String.class),
            translatedOperands.get(0),
            Expressions.constant(","));
      } else if (translatedOperands.size() == 2) {
        // ARRAY_TO_CSV(array, delimiter)
        return Expressions.call(
            Types.lookupMethod(
                ArrayToCsvFunctionImpl.class, "arrayToCsv", List.class, String.class),
            translatedOperands.get(0),
            translatedOperands.get(1));
      } else {
        throw new IllegalArgumentException(
            "ARRAY_TO_CSV expects 1 or 2 arguments, got " + translatedOperands.size());
      }
    }
  }

  /**
   * Converts an array to a CSV string.
   *
   * @param array The array to convert
   * @param delimiter The delimiter to use for joining values
   * @return CSV string representation of the array
   */
  public static String arrayToCsv(List<Object> array, String delimiter) {
    if (array == null) {
      return null;
    }

    if (delimiter == null) {
      delimiter = ",";
    }

    if (array.isEmpty()) {
      return "";
    }

    StringBuilder result = new StringBuilder();
    for (int i = 0; i < array.size(); i++) {
      if (i > 0) {
        result.append(delimiter);
      }
      Object element = array.get(i);
      if (element != null) {
        result.append(element.toString());
      }
    }

    return result.toString();
  }
}
