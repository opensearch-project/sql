/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.expression.function.PPLTypeChecker.PPLComparableTypeChecker;

/**
 * Exercises {@link PPLComparableTypeChecker#checkOperandTypes} against representative type pairs.
 */
class PPLComparableTypeCheckerTest {

  private static final OpenSearchTypeFactory TF = OpenSearchTypeFactory.TYPE_FACTORY;
  private static final PPLComparableTypeChecker CHECKER =
      new PPLComparableTypeChecker((SameOperandTypeChecker) OperandTypes.SAME_SAME);

  private static RelDataType sql(SqlTypeName name) {
    return TF.createSqlType(name);
  }

  private static RelDataType udt(ExprUDT udt) {
    return TF.createUDT(udt);
  }

  private static RelDataType udt(ExprUDT udt, boolean nullable) {
    return TF.createUDT(udt, nullable);
  }

  private static RelDataType interval(TimeUnit unit) {
    return TF.createSqlIntervalType(new SqlIntervalQualifier(unit, unit, SqlParserPos.ZERO));
  }

  private static boolean comparable(RelDataType a, RelDataType b) {
    return CHECKER.checkOperandTypes(List.of(a, b));
  }

  @Test
  void numericAndNumericAreComparable() {
    assertTrue(comparable(sql(SqlTypeName.INTEGER), sql(SqlTypeName.DOUBLE)));
    assertTrue(comparable(sql(SqlTypeName.TINYINT), sql(SqlTypeName.BIGINT)));
  }

  @Test
  void sameUdtIsComparable() {
    assertTrue(comparable(udt(ExprUDT.EXPR_DATE), udt(ExprUDT.EXPR_DATE)));
    assertTrue(comparable(udt(ExprUDT.EXPR_TIMESTAMP, true), udt(ExprUDT.EXPR_TIMESTAMP, false)));
  }

  @Test
  void plainBinaryVsBinaryUdtAreComparable() {
    // Guardrail: any future refactor of isComparable that classifies via SqlTypeFamily or Java
    // class would break this pair — VARBINARY (family=BINARY) and EXPR_BINARY (VARCHAR-backed
    // UDT) look unrelated at that level, but both map to ExprCoreType.BINARY and must compare.
    assertTrue(comparable(sql(SqlTypeName.VARBINARY), udt(ExprUDT.EXPR_BINARY)));
    assertTrue(comparable(udt(ExprUDT.EXPR_BINARY), sql(SqlTypeName.VARBINARY)));
  }

  @Test
  void dayTimeAndYearMonthIntervalsAreComparable() {
    // Guardrail: Calcite splits INTERVAL into day-time and year-month SqlTypeFamilies, so a
    // family-based check would reject this pair. convertRelDataTypeToExprType collapses every
    // interval SqlTypeName to ExprCoreType.INTERVAL, so shouldCast is false and both compare.
    assertTrue(comparable(interval(TimeUnit.DAY), interval(TimeUnit.YEAR)));
    assertTrue(comparable(interval(TimeUnit.HOUR), interval(TimeUnit.MONTH)));
  }

  @Test
  void plainTemporalVsMatchingTemporalUdtIsComparable() {
    assertTrue(comparable(sql(SqlTypeName.TIMESTAMP), udt(ExprUDT.EXPR_TIMESTAMP)));
    assertTrue(comparable(sql(SqlTypeName.DATE), udt(ExprUDT.EXPR_DATE)));
    assertTrue(comparable(sql(SqlTypeName.TIME), udt(ExprUDT.EXPR_TIME)));
  }

  @Test
  void udtVsUnrelatedPlainTypeIsNotComparable() {
    assertFalse(comparable(udt(ExprUDT.EXPR_DATE), sql(SqlTypeName.VARCHAR)));
    assertFalse(comparable(udt(ExprUDT.EXPR_TIMESTAMP), sql(SqlTypeName.INTEGER)));
  }

  @Test
  void stringVsNumericIsNotComparable() {
    assertFalse(comparable(sql(SqlTypeName.VARCHAR), sql(SqlTypeName.INTEGER)));
  }

  @Test
  void anyIsComparableWithAnything() {
    assertTrue(comparable(sql(SqlTypeName.ANY), sql(SqlTypeName.INTEGER)));
    assertTrue(comparable(sql(SqlTypeName.ANY), udt(ExprUDT.EXPR_DATE)));
  }

  @Test
  void structVsNonStructIsNotComparable() {
    RelDataType struct =
        TF.createStructType(
            List.of(sql(SqlTypeName.INTEGER), sql(SqlTypeName.VARCHAR)), List.of("a", "b"));
    assertFalse(comparable(struct, sql(SqlTypeName.INTEGER)));
  }

  @Test
  void structsWithMatchingFieldsAreComparable() {
    RelDataType s1 =
        TF.createStructType(
            List.of(sql(SqlTypeName.INTEGER), sql(SqlTypeName.VARCHAR)), List.of("a", "b"));
    RelDataType s2 =
        TF.createStructType(
            List.of(sql(SqlTypeName.BIGINT), sql(SqlTypeName.CHAR)), List.of("x", "y"));
    assertTrue(comparable(s1, s2));
  }

  @Test
  void structsWithMismatchedFieldCountsAreNotComparable() {
    RelDataType s1 = TF.createStructType(List.of(sql(SqlTypeName.INTEGER)), List.of("a"));
    RelDataType s2 =
        TF.createStructType(
            List.of(sql(SqlTypeName.INTEGER), sql(SqlTypeName.VARCHAR)), List.of("a", "b"));
    assertFalse(comparable(s1, s2));
  }

  @Test
  void ipTypesAreRejectedByOuterChecker() {
    // IP UDTs are explicitly filtered out in PPLComparableTypeChecker.checkOperandTypes so that
    // built-in comparable functions (COALESCE, NULLIF, IFNULL, IF) cannot accept them.
    assertFalse(comparable(udt(ExprUDT.EXPR_IP), udt(ExprUDT.EXPR_IP)));
  }
}
