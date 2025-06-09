/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

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
        return String.join(",", allowedSignatures);
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
      return String.join(",", allowedSignatures);
    }
  }

  @RequiredArgsConstructor
  class PPLComparableTypeChecker implements PPLTypeChecker {
    private final SameOperandTypeChecker innerTypeChecker;

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      if (!innerTypeChecker.getOperandCountRange().isValidCount(types.size())) {
        return false;
      }
      // Check comparability of consecutive operands
      for (int i = 0; i < types.size() - 1; i++) {
        // TODO: Binary, Array UDT?
        // DATETIME, NUMERIC, BOOLEAN will be regarded as comparable
        // with strings in SqlTypeUtil.isComparable
        RelDataType type_l = types.get(i);
        RelDataType type_r = types.get(i + 1);
        if (!SqlTypeUtil.isComparable(type_l, type_r)) {
          if (areIpAndStringTypes(type_l, type_r) || areIpAndStringTypes(type_r, type_l)) {
            // Allow IP and string comparison
            continue;
          }
          return false;
        }
        // Disallow coercing between strings and numeric, boolean
        if ((type_l.getFamily() == SqlTypeFamily.CHARACTER
                && cannotConvertStringInCompare((SqlTypeFamily) type_r.getFamily()))
            || (type_r.getFamily() == SqlTypeFamily.CHARACTER
                && cannotConvertStringInCompare((SqlTypeFamily) type_l.getFamily()))) {
          return false;
        }
      }
      return true;
    }

    private static boolean cannotConvertStringInCompare(SqlTypeFamily typeFamily) {
      return switch (typeFamily) {
        case BOOLEAN, INTEGER, NUMERIC, EXACT_NUMERIC, APPROXIMATE_NUMERIC -> true;
        default -> false;
      };
    }

    private static boolean areIpAndStringTypes(RelDataType typeIp, RelDataType typeString) {
      if (typeIp instanceof AbstractExprRelDataType<?> exprRelDataType) {
        return exprRelDataType.getExprType() == ExprCoreType.IP
            && typeString.getFamily() == SqlTypeFamily.CHARACTER;
      }
      return false;
    }

    @Override
    public String getAllowedSignatures() {
      int min = innerTypeChecker.getOperandCountRange().getMin();
      int max = innerTypeChecker.getOperandCountRange().getMax();
      final String typeName = "COMPARABLE_TYPE";
      if (min == -1 || max == -1) {
        // If the range is unbounded, we cannot provide a specific signature
        return String.format("[%s...]", typeName);
      } else {
        // Generate a signature based on the min and max operand counts
        List<String> signatures = new ArrayList<>();
        // avoid enumerating too many signatures
        final int MAX_ARGS = 10;
        max = Math.min(MAX_ARGS, max);
        for (int i = min; i <= max; i++) {
          signatures.add("[" + String.join(",", Collections.nCopies(i, typeName)) + "]");
        }
        return String.join(",", signatures);
      }
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
   * <p>Additionally, if {@code checkCompositionType} is true, the method checks if the composition
   * type of the provided {@code CompositeOperandTypeChecker} is OR via reflection. If it is not, an
   * {@link IllegalArgumentException} is thrown. If the reflective access to the composition field
   * of CompositeOperandTypeChecker fails, an {@link UnsupportedOperationException} is thrown.
   *
   * @param typeChecker the Calcite {@link CompositeOperandTypeChecker} to wrap
   * @param checkCompositionType if true, checks if the composition type is OR.
   * @return a {@link PPLCompositeTypeChecker} that delegates type checking to the wrapped rules
   * @throws IllegalArgumentException if any rule is not an {@link ImplicitCastOperandTypeChecker}
   */
  static PPLCompositeTypeChecker wrapComposite(
      CompositeOperandTypeChecker typeChecker, boolean checkCompositionType)
      throws IllegalArgumentException, UnsupportedOperationException {
    if (checkCompositionType) {
      try {
        if (!isCompositionOr(typeChecker)) {
          throw new IllegalArgumentException(
              "Currently only support CompositeOperandTypeChecker with a OR composition");
        }
      } catch (ReflectiveOperationException | InaccessibleObjectException | SecurityException e) {
        throw new UnsupportedOperationException(
            String.format("Failed to check composition type of %s", typeChecker), e);
      }
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

  static PPLComparableTypeChecker wrapComparable(SameOperandTypeChecker typeChecker) {
    return new PPLComparableTypeChecker(typeChecker);
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
   * Converts a {@link SqlTypeFamily} to a list of {@link ExprType}. This method is used to display
   * the allowed signatures for functions based on their type families.
   *
   * @param family the {@link SqlTypeFamily} to convert
   * @return a list of {@link ExprType} corresponding to the concrete types of the family
   */
  private static List<ExprType> getExprTypes(SqlTypeFamily family) {
    List<RelDataType> concreteTypes =
        switch (family) {
          case DATETIME -> List.of(
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DATE),
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.TIME),
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP));
          case NUMERIC -> List.of(
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE));
            // Integer is mapped to BIGINT in family.getDefaultConcreteType
          case INTEGER -> List.of(
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
          case ANY, IGNORE -> List.of(
              OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.ANY));
          default -> {
            RelDataType type = family.getDefaultConcreteType(OpenSearchTypeFactory.TYPE_FACTORY);
            if (type == null) {
              yield List.of(OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.OTHER));
            }
            yield List.of(type);
          }
        };
    return concreteTypes.stream()
        .map(OpenSearchTypeFactory::convertRelDataTypeToExprType)
        .collect(Collectors.toList());
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
    List<List<ExprType>> exprTypes =
        families.stream().map(PPLTypeChecker::getExprTypes).collect(Collectors.toList());

    // Do a cartesian product of all ExprTypes in the family
    List<List<ExprType>> signatures = Lists.cartesianProduct(exprTypes);

    // Convert each signature to a string representation and then concatenate them
    return signatures.stream()
        .map(
            types ->
                "["
                    + types.stream()
                        // Display ExprCoreType.UNDEFINED as "ANY" for better interpretability
                        .map(t -> t == ExprCoreType.UNDEFINED ? "ANY" : t.toString())
                        .collect(Collectors.joining(","))
                    + "]")
        .collect(Collectors.joining(","));
  }

  /**
   * Checks if the provided {@link CompositeOperandTypeChecker} is of type OR composition.
   *
   * <p>This method uses reflection to access the protected "composition" field of the
   * CompositeOperandTypeChecker class.
   *
   * @param typeChecker the CompositeOperandTypeChecker to check
   * @return true if the composition is OR, false otherwise
   */
  private static boolean isCompositionOr(CompositeOperandTypeChecker typeChecker)
      throws NoSuchFieldException,
          IllegalAccessException,
          InaccessibleObjectException,
          SecurityException {
    Field compositionField = CompositeOperandTypeChecker.class.getDeclaredField("composition");
    compositionField.setAccessible(true);
    CompositeOperandTypeChecker.Composition composition =
        (CompositeOperandTypeChecker.Composition) compositionField.get(typeChecker);
    return composition == CompositeOperandTypeChecker.Composition.OR;
  }
}
