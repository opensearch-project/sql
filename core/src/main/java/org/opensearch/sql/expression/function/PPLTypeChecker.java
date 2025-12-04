/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.opensearch.sql.calcite.type.ExprIPType;
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
@Deprecated
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
   * [STRING,STRING]|[INTEGER,INTEGER]}.
   *
   * @return a string representation of the allowed signatures
   */
  String getAllowedSignatures();

  /**
   * Get a list of all possible parameter type combinations for the function.
   *
   * <p>This method is used to generate the allowed signatures for the function based on the
   * parameter types.
   *
   * @return a list of lists, where each inner list represents an allowed parameter type combination
   */
  List<List<ExprType>> getParameterTypes();

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
    public List<List<ExprType>> getParameterTypes() {
      return PPLTypeChecker.getExprSignatures(families);
    }

    @Override
    public String toString() {
      return String.format("PPLFamilyTypeChecker[families=%s]", getAllowedSignatures());
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
        // with strings in isComparable
        RelDataType type_l = types.get(i);
        RelDataType type_r = types.get(i + 1);
        // Rule out IP types from built-in comparable functions
        if (type_l instanceof ExprIPType || type_r instanceof ExprIPType) {
          return false;
        }
        if (!isComparable(type_l, type_r)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Modified from {@link SqlTypeUtil#isComparable(RelDataType, RelDataType)} to
     *
     * @param type1 first type
     * @param type2 second type
     * @return true if the two types are comparable, false otherwise
     */
    private static boolean isComparable(RelDataType type1, RelDataType type2) {
      if (type1.isStruct() != type2.isStruct()) {
        return false;
      }

      if (type1.isStruct()) {
        int n = type1.getFieldCount();
        if (n != type2.getFieldCount()) {
          return false;
        }
        for (Pair<RelDataTypeField, RelDataTypeField> pair :
            Pair.zip(type1.getFieldList(), type2.getFieldList())) {
          if (!isComparable(pair.left.getType(), pair.right.getType())) {
            return false;
          }
        }
        return true;
      }

      // Numeric types are comparable without the need to cast
      if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
        return true;
      }

      ExprType exprType1 = OpenSearchTypeFactory.convertRelDataTypeToExprType(type1);
      ExprType exprType2 = OpenSearchTypeFactory.convertRelDataTypeToExprType(type2);

      if (!exprType1.shouldCast(exprType2)) {
        return true;
      }

      // If one of the arguments is of type 'ANY', return true.
      return type1.getFamily() == SqlTypeFamily.ANY || type2.getFamily() == SqlTypeFamily.ANY;
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

    @Override
    public List<List<ExprType>> getParameterTypes() {
      // Should not be used
      return List.of(List.of(ExprCoreType.UNKNOWN, ExprCoreType.UNKNOWN));
    }
  }

  class PPLDefaultTypeChecker implements PPLTypeChecker {
    private final SqlOperandTypeChecker internal;

    public PPLDefaultTypeChecker(SqlOperandTypeChecker typeChecker) {
      internal = typeChecker;
    }

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      // Basic operand count validation
      if (!internal.getOperandCountRange().isValidCount(types.size())) {
        return false;
      }

      // If the internal checker is a FamilyOperandTypeChecker, use type family validation
      if (internal instanceof FamilyOperandTypeChecker familyChecker) {
        List<SqlTypeFamily> families =
            IntStream.range(0, types.size())
                .mapToObj(familyChecker::getOperandSqlTypeFamily)
                .collect(Collectors.toList());
        return validateOperands(families, types);
      }

      // For other types of checkers, we can only validate operand count
      // This is a fallback - we assume the types are valid if count is correct
      return true;
    }

    @Override
    public String getAllowedSignatures() {
      if (internal instanceof FamilyOperandTypeChecker familyChecker) {
        return getFamilySignatures(familyChecker);
      } else {
        // Generate a generic signature based on operand count range
        int min = internal.getOperandCountRange().getMin();
        int max = internal.getOperandCountRange().getMax();

        if (min == -1 || max == -1) {
          return "[ANY...]";
        } else if (min == max) {
          return "[" + String.join(",", Collections.nCopies(min, "ANY")) + "]";
        } else {
          List<String> signatures = new ArrayList<>();
          final int MAX_ARGS = 10;
          max = Math.min(MAX_ARGS, max);
          for (int i = min; i <= max; i++) {
            signatures.add("[" + String.join(",", Collections.nCopies(i, "ANY")) + "]");
          }
          return String.join("|", signatures);
        }
      }
    }

    @Override
    public List<List<ExprType>> getParameterTypes() {
      if (internal instanceof FamilyOperandTypeChecker familyChecker) {
        return getExprSignatures(familyChecker);
      } else {
        // For unknown type checkers, return UNKNOWN types
        int min = internal.getOperandCountRange().getMin();
        int max = internal.getOperandCountRange().getMax();

        if (min == -1 || max == -1) {
          // Variable arguments - return a single signature with UNKNOWN
          return List.of(List.of(ExprCoreType.UNKNOWN));
        } else {
          List<List<ExprType>> parameterTypes = new ArrayList<>();
          final int MAX_ARGS = 10;
          max = Math.min(MAX_ARGS, max);
          for (int i = min; i <= max; i++) {
            parameterTypes.add(Collections.nCopies(i, ExprCoreType.UNKNOWN));
          }
          return parameterTypes;
        }
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

  static PPLComparableTypeChecker wrapComparable(SameOperandTypeChecker typeChecker) {
    return new PPLComparableTypeChecker(typeChecker);
  }

  /**
   * Create a {@link PPLTypeChecker} from a list of allowed signatures consisted of {@link
   * ExprType}. This is useful to validate arguments against user-defined types (UDT) that does not
   * match any Calcite {@link SqlTypeFamily}.
   *
   * @param allowedSignatures a list of allowed signatures, where each signature is a list of {@link
   *     ExprType} representing the expected types of the function arguments.
   * @return a {@link PPLTypeChecker} that checks if the operand types match any of the allowed
   *     signatures
   */
  static PPLTypeChecker wrapUDT(List<List<ExprType>> allowedSignatures) {
    return new PPLTypeChecker() {
      @Override
      public boolean checkOperandTypes(List<RelDataType> types) {
        List<ExprType> argExprTypes =
            types.stream().map(OpenSearchTypeFactory::convertRelDataTypeToExprType).toList();
        for (var allowedSignature : allowedSignatures) {
          if (allowedSignature.size() != types.size()) {
            continue; // Skip signatures that do not match the operand count
          }
          // Check if the argument types match the allowed signature
          if (IntStream.range(0, allowedSignature.size())
              .allMatch(i -> allowedSignature.get(i).equals(argExprTypes.get(i)))) {
            return true;
          }
        }
        return false;
      }

      @Override
      public String getAllowedSignatures() {
        return PPLTypeChecker.formatExprSignatures(allowedSignatures);
      }

      @Override
      public List<List<ExprType>> getParameterTypes() {
        return allowedSignatures;
      }
    };
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
  private static String getFamilySignatures(FamilyOperandTypeChecker typeChecker) {
    var allowedExprSignatures = getExprSignatures(typeChecker);
    return formatExprSignatures(allowedExprSignatures);
  }

  private static List<List<ExprType>> getExprSignatures(FamilyOperandTypeChecker typeChecker) {
    var operandCountRange = typeChecker.getOperandCountRange();
    int min = operandCountRange.getMin();
    int max = operandCountRange.getMax();
    List<SqlTypeFamily> families = new ArrayList<>();
    for (int i = 0; i < min; i++) {
      families.add(typeChecker.getOperandSqlTypeFamily(i));
    }
    List<List<ExprType>> allowedSignatures = new ArrayList<>(getExprSignatures(families));

    // Avoid enumerating signatures for infinite args
    final int MAX_ARGS = 10;
    max = Math.min(max, MAX_ARGS);
    for (int i = min; i < max; i++) {
      families.add(typeChecker.getOperandSqlTypeFamily(i));
      allowedSignatures.addAll(getExprSignatures(families));
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
          case DATETIME ->
              List.of(
                  OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP),
                  OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DATE),
                  OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.TIME));
          case NUMERIC ->
              List.of(
                  OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                  OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE));
          // Integer is mapped to BIGINT in family.getDefaultConcreteType
          case INTEGER ->
              List.of(OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
          case ANY, IGNORE ->
              List.of(OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.ANY));
          case DATETIME_INTERVAL ->
              SqlTypeName.INTERVAL_TYPES.stream()
                  .map(
                      type ->
                          OpenSearchTypeFactory.TYPE_FACTORY.createSqlIntervalType(
                              new SqlIntervalQualifier(
                                  type.getStartUnit(), type.getEndUnit(), SqlParserPos.ZERO)))
                  .collect(Collectors.toList());
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
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Generates a list of all possible {@link ExprType} signatures based on the provided {@link
   * SqlTypeFamily} list.
   *
   * @param families the list of {@link SqlTypeFamily} to generate signatures for
   * @return a list of lists, where each inner list contains {@link ExprType} signatures
   */
  private static List<List<ExprType>> getExprSignatures(List<SqlTypeFamily> families) {
    List<List<ExprType>> exprTypes =
        families.stream().map(PPLTypeChecker::getExprTypes).collect(Collectors.toList());

    // Do a cartesian product of all ExprTypes in the family
    return Lists.cartesianProduct(exprTypes);
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
    List<List<ExprType>> signatures = getExprSignatures(families);
    // Convert each signature to a string representation and then concatenate them
    return formatExprSignatures(signatures);
  }

  private static String formatExprSignatures(List<List<ExprType>> signatures) {
    return signatures.stream()
        .map(
            types ->
                "["
                    + types.stream()
                        // Display ExprCoreType.UNDEFINED as "ANY" for better interpretability
                        .map(t -> t == ExprCoreType.UNDEFINED ? "ANY" : t.toString())
                        .collect(Collectors.joining(","))
                    + "]")
        .collect(Collectors.joining("|"));
  }
}
