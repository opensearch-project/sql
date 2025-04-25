/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

public interface PPLTypeChecker {
  boolean checkOperandTypes(List<RelDataType> types);

  private static boolean validateOperands(
      List<SqlTypeFamily> funcTypeFamilies, List<RelDataType> operandTypes) {
    if (funcTypeFamilies.size() != operandTypes.size()) {
      return true; // Skip checking if sizes do not match because some arguments may be optional
    }
    for (int i = 0; i < operandTypes.size(); i++) {
      SqlTypeName paramType = UserDefinedFunctionUtils.convertRelDataTypeToSqlTypeName(operandTypes.get(i));
      SqlTypeFamily funcTypeFamily = funcTypeFamilies.get(i);
      if (paramType.getFamily() == SqlTypeFamily.IGNORE || funcTypeFamily == SqlTypeFamily.IGNORE) {
        continue;
      }
      if (!funcTypeFamily.getTypeNames().contains(paramType)) {
        return false;
      }
    }
    return true;
  }

  class PPLFamilyTypeChecker implements PPLTypeChecker {
    private final List<SqlTypeFamily> families;

    public PPLFamilyTypeChecker(SqlTypeFamily... families) {
      this.families = List.of(families);
    }

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      if (families.size() != types.size()) return false;
      return validateOperands(families, types);
    }
  }

  class PPLFamilyTypeCheckerWrapper implements PPLTypeChecker {
    protected final ImplicitCastOperandTypeChecker innerTypeChecker;

    public PPLFamilyTypeCheckerWrapper(ImplicitCastOperandTypeChecker typeChecker) {
      this.innerTypeChecker = typeChecker;
    }

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      if (innerTypeChecker instanceof SqlOperandTypeChecker sqlOperandTypeChecker
          && !sqlOperandTypeChecker.getOperandCountRange().isValidCount(types.size()))
        return false;
      List<SqlTypeFamily> families =
          IntStream.range(0, types.size())
              .mapToObj(innerTypeChecker::getOperandSqlTypeFamily)
              .collect(Collectors.toList());
      return validateOperands(families, types);
    }
  }

  /** Creates a checker that passes if each operand is a member of a corresponding family */
  static PPLFamilyTypeChecker family(SqlTypeFamily... families) {
    return new PPLFamilyTypeChecker(families);
  }

  static PPLFamilyTypeCheckerWrapper familyWrapper(ImplicitCastOperandTypeChecker typeChecker) {
    return new PPLFamilyTypeCheckerWrapper(typeChecker);
  }
}
