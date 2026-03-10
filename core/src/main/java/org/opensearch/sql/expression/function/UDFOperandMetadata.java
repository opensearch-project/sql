/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * This class is created for the compatibility with {@link SqlUserDefinedFunction} constructors when
 * creating UDFs, so that a type checker can be passed to the constructor of {@link
 * SqlUserDefinedFunction} as a {@link SqlOperandMetadata}.
 */
public interface UDFOperandMetadata extends SqlOperandMetadata {
  SqlOperandTypeChecker getInnerTypeChecker();

  static UDFOperandMetadata wrap(SqlOperandTypeChecker typeChecker) {
    return new UDFOperandMetadata() {
      @Override
      public SqlOperandTypeChecker getInnerTypeChecker() {
        return typeChecker;
      }

      @Override
      public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        // This function is not used in the current context
        throw new UnsupportedOperationException(
            "paramTypes of UDFOperandMetadata is not implemented and should not be called");
      }

      @Override
      public List<String> paramNames() {
        // This function is not used in the current context
        throw new UnsupportedOperationException(
            "paramNames of UDFOperandMetadata is not implemented and should not be called");
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
}
