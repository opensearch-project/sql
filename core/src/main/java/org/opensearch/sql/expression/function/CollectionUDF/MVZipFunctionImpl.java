/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MVZip function that combines two multivalue fields pairwise with a delimiter.
 *
 * <p>This function zips together two arrays by combining the first value of left with the first
 * value of right, the second with the second, and so on, up to the length of the shorter array.
 *
 * <p>Behavior:
 *
 * <ul>
 *   <li>Returns null if either left or right is null
 *   <li>Returns an empty array if one or both arrays are empty
 *   <li>Stops at the length of the shorter array (like Python's zip)
 *   <li>Uses the provided delimiter to join values (default: ",")
 * </ul>
 */
public class MVZipFunctionImpl extends ImplementorUDF {

  public MVZipFunctionImpl() {
    // Use ANY: return null if any argument is null
    super(new MVZipImplementor(), NullPolicy.ANY);
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
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    // First two arguments must be arrays, optional STRING delimiter
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER)));
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
                MVZipFunctionImpl.class, "mvzip", List.class, List.class, String.class),
            translatedOperands.get(0),
            translatedOperands.get(1),
            Expressions.constant(","));
      } else if (translatedOperands.size() == 3) {
        // mvzip(left, right, delimiter)
        return Expressions.call(
            Types.lookupMethod(
                MVZipFunctionImpl.class, "mvzip", List.class, List.class, String.class),
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
   * Combines two multivalue arrays pairwise with a delimiter.
   *
   * @param left The left multivalue array
   * @param right The right multivalue array
   * @param delimiter The delimiter to use for joining values (default: ",")
   * @return A list of combined values, empty list if either array is empty, or null if either input
   *     is null
   */
  public static List<Object> mvzip(List<?> left, List<?> right, String delimiter) {
    if (left == null || right == null) {
      return null;
    }

    List<Object> result = new ArrayList<>();
    int minLength = Math.min(left.size(), right.size());

    for (int i = 0; i < minLength; i++) {
      String combined =
          Objects.toString(left.get(i), "") + delimiter + Objects.toString(right.get(i), "");
      result.add(combined);
    }

    return result;
  }
}
