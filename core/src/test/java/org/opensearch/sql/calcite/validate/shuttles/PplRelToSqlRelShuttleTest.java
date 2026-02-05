/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PplRelToSqlRelShuttleTest {

  private RexBuilder rexBuilder;

  @BeforeEach
  public void setUp() {
    rexBuilder = new RexBuilder(TYPE_FACTORY);
  }

  // ==================== Float literal tests ====================

  @Test
  public void testVisitLiteral_realTypeLiteral_wrapsSafeCast() {
    RexShuttle rexShuttle = createRexShuttle(true);

    // Create a REAL type literal
    RelDataType realType = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
    RexLiteral floatLiteral = (RexLiteral) rexBuilder.makeLiteral(3.14f, realType, true);

    RexNode result = floatLiteral.accept(rexShuttle);

    // Should be wrapped in SAFE_CAST
    assertInstanceOf(RexCall.class, result);
    RexCall castCall = (RexCall) result;
    assertEquals(SqlLibraryOperators.SAFE_CAST, castCall.getOperator());
    assertEquals(floatLiteral, castCall.getOperands().get(0));
    assertEquals(SqlTypeName.REAL, castCall.getType().getSqlTypeName());
  }

  @Test
  public void testVisitLiteral_floatTypeLiteral_wrapsSafeCast() {
    RexShuttle rexShuttle = createRexShuttle(true);

    // Create a FLOAT type literal (FLOAT is an alias for REAL in SQL)
    RelDataType floatType = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);
    RexLiteral floatLiteral = (RexLiteral) rexBuilder.makeLiteral(2.71f, floatType, true);

    RexNode result = floatLiteral.accept(rexShuttle);

    // Should be wrapped in SAFE_CAST
    assertInstanceOf(RexCall.class, result);
    RexCall castCall = (RexCall) result;
    assertEquals(SqlLibraryOperators.SAFE_CAST, castCall.getOperator());
    assertTrue(SqlTypeUtil.isFlat(castCall.getType()));
  }

  @Test
  public void testVisitLiteral_doubleLiteral_remainsUnchanged() {
    RexShuttle rexShuttle = createRexShuttle(true);

    // Create a DOUBLE type literal - should NOT be wrapped
    RelDataType doubleType = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);
    RexLiteral doubleLiteral = (RexLiteral) rexBuilder.makeLiteral(3.14159d, doubleType, true);

    RexNode result = doubleLiteral.accept(rexShuttle);

    // Double literals should remain unchanged
    assertInstanceOf(RexLiteral.class, result);
    assertEquals(doubleLiteral, result);
  }

  // ==================== Interval literal tests ====================

  @Test
  public void testVisitLiteral_intervalDayLiteral_forwardMultiplies() {
    RexShuttle rexShuttle = createRexShuttle(true);

    // Create an INTERVAL DAY literal with value 5
    SqlIntervalQualifier dayQualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
    RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(5), dayQualifier);

    RexNode result = intervalLiteral.accept(rexShuttle);

    assertInstanceOf(RexLiteral.class, result);
    RexLiteral resultLiteral = (RexLiteral) result;
    assertNotNull(resultLiteral.getType().getIntervalQualifier());

    // Forward multiplies by DAY multiplier (86400000 ms)
    BigDecimal resultValue = resultLiteral.getValueAs(BigDecimal.class);
    assertNotNull(resultValue);
    BigDecimal expectedValue = BigDecimal.valueOf(5).multiply(TimeUnit.DAY.multiplier);
    assertEquals(0, expectedValue.compareTo(resultValue));
  }

  @Test
  public void testVisitLiteral_intervalHourLiteral_backwardDivides() {
    RexShuttle rexShuttle = createRexShuttle(false);

    // Create an INTERVAL HOUR literal with value in ms (2 hours = 2 * 3600000 ms)
    SqlIntervalQualifier hourQualifier =
        new SqlIntervalQualifier(TimeUnit.HOUR, null, SqlParserPos.ZERO);
    BigDecimal msValue = BigDecimal.valueOf(2L * 3600000L);
    RexLiteral intervalLiteral = rexBuilder.makeIntervalLiteral(msValue, hourQualifier);

    RexNode result = intervalLiteral.accept(rexShuttle);

    assertInstanceOf(RexLiteral.class, result);
    RexLiteral resultLiteral = (RexLiteral) result;

    // Backward divides to get back to original unit
    BigDecimal resultValue = resultLiteral.getValueAs(BigDecimal.class);
    assertNotNull(resultValue);
    // 7200000 / 3600000 = 2
    assertEquals(0, BigDecimal.valueOf(2).compareTo(resultValue));
  }

  @Test
  public void testVisitLiteral_intervalMonthLiteral_forwardMultiplies() {
    RexShuttle rexShuttle = createRexShuttle(true);

    // Create an INTERVAL MONTH literal
    SqlIntervalQualifier monthQualifier =
        new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO);
    RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(3), monthQualifier);

    RexNode result = intervalLiteral.accept(rexShuttle);

    assertInstanceOf(RexLiteral.class, result);
    RexLiteral resultLiteral = (RexLiteral) result;

    // MONTH multiplier is 1 (months are stored as month count)
    BigDecimal resultValue = resultLiteral.getValueAs(BigDecimal.class);
    assertNotNull(resultValue);
    assertEquals(0, BigDecimal.valueOf(3).compareTo(resultValue));
  }

  @Test
  public void testVisitLiteral_intervalQuarterLiteral_usesSpecialMultiplier() {
    RexShuttle rexShuttle = createRexShuttle(true);

    // Create an INTERVAL QUARTER literal - has special handling
    SqlIntervalQualifier quarterQualifier =
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO);
    RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(2), quarterQualifier);

    RexNode result = intervalLiteral.accept(rexShuttle);

    assertInstanceOf(RexLiteral.class, result);
    RexLiteral resultLiteral = (RexLiteral) result;
    BigDecimal resultValue = resultLiteral.getValueAs(BigDecimal.class);
    assertNotNull(resultValue);

    // For forward=true with QUARTER, uses forwardMultiplier of 1 instead of 3
    // This is the fix for Calcite bug where QUARTER returns months instead of quarters
    assertEquals(0, BigDecimal.valueOf(2).compareTo(resultValue));
  }

  @Test
  public void testVisitLiteral_intervalQuarterLiteral_backwardUsesNormalMultiplier() {
    RexShuttle rexShuttle = createRexShuttle(false);

    // Create an INTERVAL QUARTER literal with value 6 (representing 6 months = 2 quarters)
    SqlIntervalQualifier quarterQualifier =
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO);
    RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(6), quarterQualifier);

    RexNode result = intervalLiteral.accept(rexShuttle);

    assertInstanceOf(RexLiteral.class, result);
    RexLiteral resultLiteral = (RexLiteral) result;
    BigDecimal resultValue = resultLiteral.getValueAs(BigDecimal.class);
    assertNotNull(resultValue);

    // Backward uses normal multiplier (3), so 6 / 3 = 2
    assertEquals(0, BigDecimal.valueOf(2).compareTo(resultValue));
  }

  // ==================== Non-interval/non-float literal tests ====================

  @Test
  public void testVisitLiteral_integerLiteral_remainsUnchanged() {
    RexShuttle rexShuttle = createRexShuttle(true);

    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexLiteral intLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf(42), intType);

    RexNode result = intLiteral.accept(rexShuttle);

    // Integer literals should remain unchanged
    assertInstanceOf(RexLiteral.class, result);
    assertEquals(intLiteral, result);
  }

  @Test
  public void testVisitLiteral_stringLiteral_remainsUnchanged() {
    RexShuttle rexShuttle = createRexShuttle(true);

    RexLiteral stringLiteral = rexBuilder.makeLiteral("hello");

    RexNode result = stringLiteral.accept(rexShuttle);

    // String literals should remain unchanged
    assertInstanceOf(RexLiteral.class, result);
    assertEquals(stringLiteral, result);
  }

  @Test
  public void testVisitLiteral_nullLiteral_remainsUnchanged() {
    RexShuttle rexShuttle = createRexShuttle(true);

    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexLiteral nullLiteral = rexBuilder.makeNullLiteral(intType);

    RexNode result = nullLiteral.accept(rexShuttle);

    // Null literals have no interval qualifier, so remain unchanged
    assertInstanceOf(RexLiteral.class, result);
    assertTrue(((RexLiteral) result).isNull());
  }

  @Test
  public void testVisitLiteral_booleanLiteral_remainsUnchanged() {
    RexShuttle rexShuttle = createRexShuttle(true);

    RexLiteral boolLiteral = rexBuilder.makeLiteral(true);

    RexNode result = boolLiteral.accept(rexShuttle);

    // Boolean literals should remain unchanged
    assertInstanceOf(RexLiteral.class, result);
    assertEquals(boolLiteral, result);
  }

  /**
   * Helper method to create the RexShuttle from PplRelToSqlRelShuttle. This extracts the
   * transformation logic for testing.
   */
  private RexShuttle createRexShuttle(boolean forward) {
    return new RexShuttle() {
      @Override
      public RexNode visitLiteral(RexLiteral literal) {
        // 1. Fix float literal
        SqlTypeName literalType = literal.getType().getSqlTypeName();
        if (SqlTypeName.REAL.equals(literalType) || SqlTypeName.FLOAT.equals(literalType)) {
          return rexBuilder.makeCall(
              literal.getType(), SqlLibraryOperators.SAFE_CAST, List.of(literal));
        }

        // 2. Fix interval literal
        SqlIntervalQualifier qualifier = literal.getType().getIntervalQualifier();
        if (qualifier == null) {
          return literal;
        }
        BigDecimal value = literal.getValueAs(BigDecimal.class);
        if (value == null) {
          return literal;
        }
        TimeUnit unit = qualifier.getUnit();
        BigDecimal forwardMultiplier =
            TimeUnit.QUARTER.equals(unit) ? BigDecimal.valueOf(1) : unit.multiplier;

        BigDecimal newValue =
            forward
                ? value.multiply(forwardMultiplier)
                : value.divideToIntegralValue(unit.multiplier);
        return rexBuilder.makeIntervalLiteral(newValue, qualifier);
      }
    };
  }
}
