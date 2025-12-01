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
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
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
  SqlOperandTypeChecker getInnerTypeChecker();

  static UDFOperandMetadata wrap(FamilyOperandTypeChecker typeChecker) {
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

  static UDFOperandMetadata wrap(CompositeOperandTypeChecker typeChecker) {
    for (SqlOperandTypeChecker rule : typeChecker.getRules()) {
      if (!(rule instanceof FamilyOperandTypeChecker)) {
        throw new IllegalArgumentException(
            "Currently only compositions of ImplicitCastOperandTypeChecker are supported");
      }
    }

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

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return null;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return "";
    }
  }
}
