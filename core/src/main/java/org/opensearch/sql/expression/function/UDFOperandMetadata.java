/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.data.type.ExprType;

/**
 * This class is created for the compatibility with {@link SqlUserDefinedFunction} constructors when
 * creating UDFs, so that a type checker can be passed to the constructor of {@link
 * SqlUserDefinedFunction} as a {@link SqlOperandMetadata}.
 */
public interface UDFOperandMetadata extends SqlOperandMetadata {
  /**
 * Provides the underlying operand type checker used by this metadata.
 *
 * @return the {@link SqlOperandTypeChecker} responsible for validating operand types
 */
SqlOperandTypeChecker getInnerTypeChecker();

  /**
   * Wraps a SqlOperandTypeChecker as UDFOperandMetadata for use with SqlUserDefinedFunction.
   *
   * <p>Delegates operand type checks, operand count range, and allowed-signature formatting to the
   * provided checker. Parameter type and name lists are empty in the returned metadata.
   *
   * @param typeChecker the operand type checker to delegate to
   * @return a UDFOperandMetadata view that delegates behavior to the given type checker
   */
  static UDFOperandMetadata wrap(SqlOperandTypeChecker typeChecker) {
    return new UDFOperandMetadata() {
      @Override
      public SqlOperandTypeChecker getInnerTypeChecker() {
        return typeChecker;
      }

      @Override
      public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        // This function is not used in the current context, so we return an empty list.
        return Collections.emptyList();
      }

      @Override
      public List<String> paramNames() {
        // This function is not used in the current context, so we return an empty list.
        return Collections.emptyList();
      }

      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return typeChecker.checkOperandTypes(callBinding, throwOnFailure);
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return typeChecker.getOperandCountRange();
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return typeChecker.getAllowedSignatures(op, opName);
      }
    };
  }

  static UDFOperandMetadata wrapUDT(List<List<ExprType>> allowSignatures) {
    return new UDTOperandMetadata(allowSignatures);
  }

  record UDTOperandMetadata(List<List<ExprType>> allowedParamTypes) implements UDFOperandMetadata {
    @Override
    public SqlOperandTypeChecker getInnerTypeChecker() {
      return this;
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
      return List.of();
    }

    @Override
    public List<String> paramNames() {
      return List.of();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return false;
    }

    /**
     * Compute the allowed operand count range from the configured UDT signatures.
     *
     * <p>Determines the smallest and largest number of parameters among all entries in
     * {@code allowedParamTypes} and returns a range spanning those values.
     *
     * @return a {@link SqlOperandCountRange} that spans the minimum and maximum parameter
     *     counts found in {@code allowedParamTypes}
     */
    @Override
    public SqlOperandCountRange getOperandCountRange() {
      int max = Integer.MIN_VALUE;
      int min = Integer.MAX_VALUE;
      for (List<ExprType> paramTypes : allowedParamTypes) {
        max = Math.max(max, paramTypes.size());
        min = Math.min(min, paramTypes.size());
      }
      return SqlOperandCountRanges.between(min, max);
    }

    /**
     * Provide the allowed signatures string for the given operator.
     *
     * <p>This implementation does not supply any signature information.
     *
     * @param op the operator for which signatures would be described
     * @param opName the display name of the operator
     * @return an empty string indicating no allowed-signature information is available
     */
    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return "";
    }
  }
}