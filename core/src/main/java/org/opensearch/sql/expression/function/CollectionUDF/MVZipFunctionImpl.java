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
 * MVZip function that combines two multivalue fields pairwise with a delimiter. Returns an array of
 * strings or null if either input is null.
 */
public class MVZipFunctionImpl extends ImplementorUDF {

  public MVZipFunctionImpl() {
    super(new MVZipImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();

      // mvzip returns an array of VARCHAR (strings)
      RelDataType elementType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      return createArrayType(
          typeFactory, typeFactory.createTypeWithNullability(elementType, true), true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class MVZipImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      // Handle both 2-argument (with default delimiter) and 3-argument cases
      if (translatedOperands.size() == 2) {
        // mvzip(left, right) - use default delimiter ","
        return Expressions.call(
            Types.lookupMethod(
                MVZipFunctionImpl.class, "mvzip", Object.class, Object.class, String.class),
            translatedOperands.get(0),
            translatedOperands.get(1),
            Expressions.constant(","));
      } else if (translatedOperands.size() == 3) {
        // mvzip(left, right, delimiter)
        return Expressions.call(
            Types.lookupMethod(
                MVZipFunctionImpl.class, "mvzip", Object.class, Object.class, String.class),
            translatedOperands.get(0),
            translatedOperands.get(1),
            translatedOperands.get(2));
      } else {
        throw new IllegalArgumentException(
            "mvzip expects 2 or 3 arguments, got " + translatedOperands.size());
      }
    }
  }

  /**
   * Entry point for mvzip function.
   *
   * @param left The left multivalue field or scalar value
   * @param right The right multivalue field or scalar value
   * @param delimiter The delimiter to use for joining values (default: ",")
   * @return A list of combined values, or null if either input is null
   */
  public static Object mvzip(Object left, Object right, String delimiter) {
    return MVZipCore.zipElements(left, right, delimiter);
  }
}
