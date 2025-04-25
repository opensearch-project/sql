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
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * This class is created for the compatibility with SqlUserDefinedFunction constructors when
 * creating UDFs.
 */
public interface UDFTypeChecker extends SqlOperandMetadata, ImplicitCastOperandTypeChecker {

  static UDFTypeChecker wrap(FamilyOperandTypeChecker typeChecker) {
    return new UDFTypeChecker() {
      @Override
      public boolean checkOperandTypesWithoutTypeCoercion(
          SqlCallBinding callBinding, boolean throwOnFailure) {
        return typeChecker.checkOperandTypesWithoutTypeCoercion(callBinding, throwOnFailure);
      }

      @Override
      public SqlTypeFamily getOperandSqlTypeFamily(int iFormalOperand) {
        return typeChecker.getOperandSqlTypeFamily(iFormalOperand);
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
        return typeChecker.checkOperandTypesWithoutTypeCoercion(callBinding, throwOnFailure);
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
