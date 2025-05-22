/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.lang.reflect.Field;
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
import org.apache.logging.log4j.LogManager;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

/**
 * A custom type checker interface for PPL (Piped Processing Language) functions.
 *
 * <p>Provides operand type validation based on specified type families, similar to Calcite's {@link
 * SqlOperandTypeChecker}, but adapted for PPL function requirements. This abstraction is necessary
 * because {@code SqlOperandTypeChecker::checkOperandTypes(SqlCallBinding, boolean)} cannot be
 * directly used for type checking at the logical plan level.
 */
public interface PPLTypeChecker {
  /**
   * Validates the operand types.
   *
   * @param types the list of operand types to validate
   * @return true if the operand types are valid, false otherwise
   */
  boolean checkOperandTypes(List<RelDataType> types);

  /**
   * Get a string representation of the allowed signatures. The format is like {@code
   * [STRING,STRING],[INTEGER,INTEGER]}.
   *
   * @return a string representation of the allowed signatures
   */
  String getAllowedSignatures();

  private static boolean validateOperands(
      List<SqlTypeFamily> funcTypeFamilies, List<RelDataType> operandTypes) {
    // If the number of actual operands does not match expectation, return false
    if (funcTypeFamilies.size() != operandTypes.size()) {
      return false;
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

  /**
   * A custom {@code PPLTypeChecker} that validates operand types against a list of {@link
   * SqlTypeFamily}. Instances can be created using {@link #family(SqlTypeFamily...)}.
   */
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

  /**
   * A {@code PPLTypeChecker} implementation that wraps a Calcite {@link
   * ImplicitCastOperandTypeChecker}.
   *
   * <p>This checker delegates operand count and type validation to the wrapped Calcite type
   * checker, allowing PPL functions to leverage Calcite's implicit casting and type family logic
   * for operand validation.
   */
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

  /**
   * A {@code PPLTypeChecker} implementation that wraps a Calcite {@link
   * CompositeOperandTypeChecker}.
   *
   * <p>This checker allows for the composition of multiple operand type checkers, enabling flexible
   * validation of operand types in PPL functions.
   *
   * <p>The implementation currently supports only OR compositions of {@link
   * ImplicitCastOperandTypeChecker}.
   */
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

  /**
   * Creates a {@link PPLFamilyTypeChecker} with a fixed operand count, validating that each operand
   * belongs to its corresponding {@link SqlTypeFamily}.
   *
   * <p>The number of provided {@code families} determines the required number of operands. Each
   * operand is checked against the type family at the same position in the array.
   *
   * @param families the expected {@link SqlTypeFamily} for each operand, in order
   * @return a {@link PPLFamilyTypeChecker} that enforces the specified type families for operands
   */
  static PPLFamilyTypeChecker family(SqlTypeFamily... families) {
    return new PPLFamilyTypeChecker(families);
  }

  /**
   * Wraps a Calcite {@link ImplicitCastOperandTypeChecker} (usually a {@link
   * FamilyOperandTypeChecker}) into a custom PPLTypeChecker of type {@link
   * PPLFamilyTypeCheckerWrapper}.
   *
   * <p>The allow operand count may be fixed or variable, depending on the wrapped type checker.
   *
   * @param typeChecker the Calcite type checker to wrap
   * @return a PPLTypeChecker that uses the wrapped type checker
   */
  static PPLFamilyTypeCheckerWrapper wrapFamily(ImplicitCastOperandTypeChecker typeChecker) {
    return new PPLFamilyTypeCheckerWrapper(typeChecker);
  }

  /**
   * Wraps a Calcite {@link CompositeOperandTypeChecker} into a custom {@link
   * PPLCompositeTypeChecker}.
   *
   * <p>This method requires that all rules within the provided {@code CompositeOperandTypeChecker}
   * are instances of {@link ImplicitCastOperandTypeChecker}. If any rule does not meet this
   * requirement, an {@link IllegalArgumentException} is thrown.
   *
   * @param typeChecker the Calcite {@link CompositeOperandTypeChecker} to wrap
   * @return a {@link PPLCompositeTypeChecker} that delegates type checking to the wrapped rules
   * @throws IllegalArgumentException if any rule is not an {@link ImplicitCastOperandTypeChecker}
   */
  static PPLCompositeTypeChecker wrapComposite(CompositeOperandTypeChecker typeChecker) {
    try {
      Field compositionField = CompositeOperandTypeChecker.class.getDeclaredField("composition");
      compositionField.setAccessible(true);
      CompositeOperandTypeChecker.Composition composition =
          (CompositeOperandTypeChecker.Composition) compositionField.get(typeChecker);
      if (composition != CompositeOperandTypeChecker.Composition.OR) {
        throw new IllegalArgumentException(
            String.format(
                "Currently only OR compositions of ImplicitCastOperandTypeChecker are supported,"
                    + " but got %s composition",
                composition.name()));
      }
    } catch (IllegalAccessException e) {
      LogManager.getLogger(PPLTypeChecker.class).error(e);
    } catch (NoSuchFieldException e) {
      LogManager.getLogger(PPLTypeChecker.class)
          .error(
              "Failed to access the composition field of CompositeOperandTypeChecker. "
                  + "This may indicate a change in the Calcite library.");
    }

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
  /**
   * Generates a list of allowed function signatures based on the provided {@link
   * FamilyOperandTypeChecker}. The signatures are generated by iterating through the operand count
   * range and collecting the corresponding type families.
   *
   * <p>If the operand count range is large, the method will limit the maximum number of signatures
   * to 10 to avoid excessive enumeration.
   *
   * @param typeChecker the {@link FamilyOperandTypeChecker} to use for generating signatures
   * @return a list of allowed function signatures
   */
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

  /**
   * Generates a string representation of the function signature based on the provided type
   * families. The format is a list of type families enclosed in square brackets, e.g.: "[INTEGER,
   * STRING]".
   *
   * @param families the list of type families to include in the signature
   * @return a string representation of the function signature
   */
  private static String getFamilySignature(List<SqlTypeFamily> families) {
    return "["
        + families.stream().map(SqlTypeFamily::toString).collect(Collectors.joining(","))
        + "]";
  }
}
