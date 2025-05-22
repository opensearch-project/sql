/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

/**
 * Type checker interface for PPL (Piped Processing Language) functions.
 *
 * <p>Provides operand type validation based on specified type families, similar to Calcite's {@link
 * SqlOperandTypeChecker}, but adapted for PPL function requirements. This abstraction is necessary
 * because {@code SqlOperandTypeChecker::checkOperandTypes(SqlCallBinding, boolean)} cannot be
 * directly used for type checking at the logical plan level.
 */
public interface PPLTypeChecker {
  boolean checkOperandTypes(List<RelDataType> types);

  String getAllowedSignatures();

  private static boolean validateOperands(
      List<SqlTypeFamily> funcTypeFamilies, List<RelDataType> operandTypes) {
    if (funcTypeFamilies.size() != operandTypes.size()) {
      return true; // Skip checking if sizes do not match because some arguments may be optional
    }
    for (int i = 0; i < operandTypes.size(); i++) {
      SqlTypeName paramType =
          UserDefinedFunctionUtils.convertRelDataTypeToSqlTypeName(operandTypes.get(i));
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

    @Override
    public String getAllowedSignatures() {
      return PPLTypeChecker.getFamilySignature(families);
    }

    @Override
    public String toString() {
      return String.format("PPLFamilyTypeChecker[families=%s]", getAllowedSignatures());
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
          && !sqlOperandTypeChecker.getOperandCountRange().isValidCount(types.size())) return false;
      List<SqlTypeFamily> families =
          IntStream.range(0, types.size())
              .mapToObj(innerTypeChecker::getOperandSqlTypeFamily)
              .collect(Collectors.toList());
      return validateOperands(families, types);
    }

    @Override
    public String getAllowedSignatures() {
      if (innerTypeChecker instanceof FamilyOperandTypeChecker familyOperandTypeChecker) {
        var allowedSignatures = PPLTypeChecker.getFamilySignatures(familyOperandTypeChecker);
        return String.join(", ", allowedSignatures);
      } else {
        return "";
      }
    }
  }

  /** Currently only support OR compositions of family type checkers. */
  class PPLCompositeTypeChecker implements PPLTypeChecker {
    private final List<? extends SqlOperandTypeChecker> allowedRules;

    public PPLCompositeTypeChecker(CompositeOperandTypeChecker typeChecker) {
      allowedRules = typeChecker.getRules();
    }

    private static boolean validateWithFamilyTypeChecker(
        SqlOperandTypeChecker checker, List<RelDataType> types) {
      if (!checker.getOperandCountRange().isValidCount(types.size())) {
        return false;
      }
      if (checker instanceof ImplicitCastOperandTypeChecker implicitCastOperandTypeChecker) {
        List<SqlTypeFamily> families =
            IntStream.range(0, types.size())
                .mapToObj(implicitCastOperandTypeChecker::getOperandSqlTypeFamily)
                .toList();
        return validateOperands(families, types);
      }
      throw new IllegalArgumentException(
          "Currently only compositions of ImplicitCastOperandTypeChecker are supported");
    }

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      boolean operandCountValid =
          allowedRules.stream()
              .anyMatch(rule -> rule.getOperandCountRange().isValidCount(types.size()));
      if (!operandCountValid) {
        return false;
      }
      return allowedRules.stream().anyMatch(rule -> validateWithFamilyTypeChecker(rule, types));
    }

    @Override
    public String getAllowedSignatures() {
      List<String> allowedSignatures = new ArrayList<>();
      for (SqlOperandTypeChecker rule : allowedRules) {
        if (rule instanceof FamilyOperandTypeChecker familyOperandTypeChecker) {
          allowedSignatures.addAll(PPLTypeChecker.getFamilySignatures(familyOperandTypeChecker));
        } else {
          throw new IllegalArgumentException(
              "Currently only compositions of FamilyOperandTypeChecker are supported");
        }
      }
      return String.join(", ", allowedSignatures);
    }
  }

  /** Creates a checker that passes if each operand is a member of a corresponding family */
  static PPLFamilyTypeChecker family(SqlTypeFamily... families) {
    return new PPLFamilyTypeChecker(families);
  }

  static PPLFamilyTypeCheckerWrapper familyWrapper(ImplicitCastOperandTypeChecker typeChecker) {
    return new PPLFamilyTypeCheckerWrapper(typeChecker);
  }

  static PPLCompositeTypeChecker compositeWrapper(CompositeOperandTypeChecker typeChecker) {
    for (SqlOperandTypeChecker rule : typeChecker.getRules()) {
      if (!(rule instanceof ImplicitCastOperandTypeChecker)) {
        throw new IllegalArgumentException(
            "Currently only compositions of ImplicitCastOperandTypeChecker are supported, found:"
                + rule.getClass().getName());
      }
    }
    return new PPLCompositeTypeChecker(typeChecker);
  }

  // Util Functions
  private static List<String> getFamilySignatures(FamilyOperandTypeChecker typeChecker) {
    var operandCountRange = typeChecker.getOperandCountRange();
    int min = operandCountRange.getMin();
    int max = operandCountRange.getMax();
    List<String> allowedSignatures = new ArrayList<>();
    List<SqlTypeFamily> families = new ArrayList<>();
    for (int i = 0; i < min; i++) {
      families.add(typeChecker.getOperandSqlTypeFamily(i));
    }
    allowedSignatures.add(getFamilySignature(families));

    // Avoid enumerating signatures for infinite args
    final int MAX_ARGS = 10;
    max = Math.min(max, MAX_ARGS);

    for (int i = min; i < max; i++) {
      families.add(typeChecker.getOperandSqlTypeFamily(i));
      allowedSignatures.add(getFamilySignature(families));
    }
    return allowedSignatures;
  }

  private static String getFamilySignature(List<SqlTypeFamily> families) {
    return "["
        + families.stream().map(SqlTypeFamily::toString).collect(Collectors.joining(","))
        + "]";
  }
}
