/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.expression.function.PPLTypeChecker.PPLSameTypeChecker;

public class PPLSameTypeCheckerTest {

  private final OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;

  // Helper methods to create RelDataTypes
  private RelDataType integerType() {
    return typeFactory.createSqlType(SqlTypeName.INTEGER);
  }

  private RelDataType doubleType() {
    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
  }

  private RelDataType stringType() {
    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  private RelDataType charType() {
    return typeFactory.createSqlType(SqlTypeName.CHAR);
  }

  private RelDataType timestampType() {
    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
  }

  private RelDataType dateType() {
    return typeFactory.createSqlType(SqlTypeName.DATE);
  }

  @Test
  public void testSameNumericFamily_DifferentTypesAllowed() {
    // Same family, different types allowed (INTEGER + DOUBLE for NUMERIC)
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.from(2), SqlTypeFamily.NUMERIC);

    // Should pass: same family (NUMERIC), different types allowed
    assertTrue(checker.checkOperandTypes(List.of(integerType(), doubleType())));
    assertTrue(checker.checkOperandTypes(List.of(doubleType(), integerType())));

    // Should fail: different families
    assertFalse(checker.checkOperandTypes(List.of(integerType(), stringType())));

    // Should fail: wrong count
    assertFalse(checker.checkOperandTypes(List.of(integerType())));
  }

  @Test
  public void testSameNumericType_ExactTypesRequired() {
    // Same family, exact same type required
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapPPLSameTypeChecker(
            SqlOperandCountRanges.from(2), SqlTypeFamily.NUMERIC, true);

    // Should pass: exact same types
    assertTrue(checker.checkOperandTypes(List.of(integerType(), integerType())));
    assertTrue(checker.checkOperandTypes(List.of(doubleType(), doubleType())));

    // Should fail: different types even in same family
    assertFalse(checker.checkOperandTypes(List.of(integerType(), doubleType())));

    // Should fail: different families
    assertFalse(checker.checkOperandTypes(List.of(integerType(), stringType())));
  }

  @Test
  public void testSameStringFamily() {
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.from(1), SqlTypeFamily.STRING);

    // Should pass: same string family
    assertTrue(checker.checkOperandTypes(List.of(stringType(), charType())));
    assertTrue(checker.checkOperandTypes(List.of(stringType())));

    // Should fail: different family
    assertFalse(checker.checkOperandTypes(List.of(stringType(), integerType())));
  }

  @Test
  public void testSameStringType() {
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapPPLSameTypeChecker(
            SqlOperandCountRanges.from(1), SqlTypeFamily.STRING, true);

    // Should pass: exact same string types
    assertTrue(checker.checkOperandTypes(List.of(stringType(), stringType())));
    assertTrue(checker.checkOperandTypes(List.of(charType(), charType())));

    // Should fail: different types in string family
    assertFalse(checker.checkOperandTypes(List.of(stringType(), charType())));
  }

  @Test
  public void testAnyFamilyExactType() {
    // Any family allowed, but all arguments must be exact same type
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapPPLSameTypeChecker(
            SqlOperandCountRanges.from(2), SqlTypeFamily.ANY, true);

    // Should pass: exact same types (any family)
    assertTrue(checker.checkOperandTypes(List.of(integerType(), integerType())));
    assertTrue(checker.checkOperandTypes(List.of(stringType(), stringType())));
    assertTrue(checker.checkOperandTypes(List.of(timestampType(), timestampType())));

    // Should fail: different types
    assertFalse(checker.checkOperandTypes(List.of(integerType(), doubleType())));
    assertFalse(checker.checkOperandTypes(List.of(stringType(), integerType())));
    assertFalse(checker.checkOperandTypes(List.of(timestampType(), dateType())));
  }

  @Test
  public void testAnyFamilySameFamily() {
    // Any family, same family within arguments (no type enforcement)
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.from(2), SqlTypeFamily.ANY);

    // Should pass: same family (any family)
    assertTrue(checker.checkOperandTypes(List.of(integerType(), doubleType()))); // NUMERIC
    assertTrue(checker.checkOperandTypes(List.of(stringType(), charType()))); // STRING
    assertFalse(checker.checkOperandTypes(List.of(timestampType(), dateType()))); // DATETIME

    // Should fail: different families
    assertFalse(checker.checkOperandTypes(List.of(integerType(), stringType())));
    assertFalse(checker.checkOperandTypes(List.of(timestampType(), integerType())));
  }

  @Test
  public void testCustomFamilyWithExactType() {
    // Custom family with exact type enforcement
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapPPLSameTypeChecker(
            SqlOperandCountRanges.between(1, 3), SqlTypeFamily.DATETIME, true);

    // Should pass: exact same datetime types
    assertTrue(checker.checkOperandTypes(List.of(timestampType(), timestampType())));
    assertTrue(checker.checkOperandTypes(List.of(dateType(), dateType())));

    // Should fail: different types in datetime family
    assertFalse(checker.checkOperandTypes(List.of(timestampType(), dateType())));

    // Should fail: non-datetime types
    assertFalse(checker.checkOperandTypes(List.of(integerType(), integerType())));
  }

  @Test
  public void testOperandCountValidation() {
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.between(2, 4), SqlTypeFamily.NUMERIC);

    // Should pass: valid counts
    assertTrue(checker.checkOperandTypes(List.of(integerType(), doubleType())));
    assertTrue(checker.checkOperandTypes(List.of(integerType(), doubleType(), integerType())));
    assertTrue(
        checker.checkOperandTypes(
            List.of(integerType(), doubleType(), integerType(), doubleType())));

    // Should fail: invalid counts
    assertFalse(checker.checkOperandTypes(List.of(integerType())));
    assertFalse(
        checker.checkOperandTypes(
            List.of(integerType(), doubleType(), integerType(), doubleType(), integerType())));
  }

  @Test
  public void testEmptyTypes() {
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.from(0), SqlTypeFamily.NUMERIC);

    // Should pass: empty types with min count 0
    assertTrue(checker.checkOperandTypes(List.of()));
  }

  @Test
  public void testGetAllowedSignatures() {
    // Test signatures for different configurations
    PPLSameTypeChecker familyChecker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.between(2, 3), SqlTypeFamily.NUMERIC);
    String familySignature = familyChecker.getAllowedSignatures();
    assertTrue(familySignature.contains("NUMERIC"));

    PPLSameTypeChecker typeChecker =
        PPLTypeChecker.wrapPPLSameTypeChecker(
            SqlOperandCountRanges.between(2, 3), SqlTypeFamily.NUMERIC, true);
    String typeSignature = typeChecker.getAllowedSignatures();
    assertTrue(typeSignature.contains("NUMERIC"));

    PPLSameTypeChecker anyExactChecker =
        PPLTypeChecker.wrapPPLSameTypeChecker(
            SqlOperandCountRanges.between(2, 3), SqlTypeFamily.ANY, true);
    String anyExactSignature = anyExactChecker.getAllowedSignatures();
    assertTrue(anyExactSignature.contains("ANY"));

    PPLSameTypeChecker anyFamilyChecker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.between(2, 3), SqlTypeFamily.ANY);
    String anyFamilySignature = anyFamilyChecker.getAllowedSignatures();
    assertTrue(anyFamilySignature.contains("ANY"));
  }

  @Test
  public void testVariableArguments() {
    // Test with variable number of arguments
    PPLSameTypeChecker checker =
        PPLTypeChecker.wrapSameFamily(SqlOperandCountRanges.from(1), SqlTypeFamily.NUMERIC);

    // Should pass: variable numeric arguments
    assertTrue(checker.checkOperandTypes(List.of(integerType())));
    assertTrue(checker.checkOperandTypes(List.of(integerType(), doubleType())));
    assertTrue(checker.checkOperandTypes(List.of(integerType(), doubleType(), integerType())));
    assertTrue(
        checker.checkOperandTypes(
            List.of(doubleType(), integerType(), doubleType(), integerType())));

    // Should fail: mixed families
    assertFalse(checker.checkOperandTypes(List.of(integerType(), stringType())));
    assertFalse(checker.checkOperandTypes(List.of(integerType(), doubleType(), stringType())));
  }

  @Test
  public void testConstructorOverloads() {
    // Test different constructor overloads
    PPLSameTypeChecker noEnforcement = new PPLSameTypeChecker(SqlOperandCountRanges.from(2));
    PPLSameTypeChecker familyOnly =
        new PPLSameTypeChecker(SqlOperandCountRanges.from(2), SqlTypeFamily.NUMERIC);
    PPLSameTypeChecker fullControl =
        new PPLSameTypeChecker(SqlOperandCountRanges.from(2), SqlTypeFamily.NUMERIC, true);

    // Test behavior differences
    List<RelDataType> mixedNumeric = List.of(integerType(), doubleType());

    // No enforcement - should pass (same family)
    assertTrue(noEnforcement.checkOperandTypes(mixedNumeric));

    // Family only - should pass (same family, different types allowed)
    assertTrue(familyOnly.checkOperandTypes(mixedNumeric));

    // Full control with exact type - should fail (different types)
    assertFalse(fullControl.checkOperandTypes(mixedNumeric));

    // Full control with same types - should pass
    assertTrue(fullControl.checkOperandTypes(List.of(integerType(), integerType())));
  }
}
