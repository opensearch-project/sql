/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_SLICE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONV;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_ITEM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LEFT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REX_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REX_EXTRACT_MULTI;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ROUND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SHA2;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBSTRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SYSDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TONUMBER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TO_SECONDS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK;

import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Test;

/**
 * Verifies BIGINT-to-INTEGER narrowing of int-domain function arguments (see {@code
 * PPLFuncImpTable.AbstractBuilder#narrowIntArgs}). PPL widens integer arithmetic to BIGINT (#5603),
 * but operators like ITEM and ROUND take a Java {@code int} at their control positions. Only the
 * positions declared at registration are narrowed.
 */
public class PPLFuncImpTableNarrowBigintTest {

  private final RexBuilder builder = new RexBuilder(TYPE_FACTORY);

  private RexNode bigint(long value) {
    return builder.makeLiteral(
        BigDecimal.valueOf(value), TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), false);
  }

  private RelDataType intArray() {
    return TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), -1);
  }

  @Test
  public void bigintIndexIsNarrowedToInteger() {
    // ITEM(array, index): index position (1) is strictly INTEGER, so a BIGINT index is narrowed.
    RexNode arrayRef = builder.makeInputRef(intArray(), 0);
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, arrayRef, bigint(1L));

    RexCall call = (RexCall) result;
    RexNode index = call.getOperands().get(1);
    // The index operand is narrowed to INTEGER (via CAST, or a folded INTEGER literal).
    assertEquals(SqlTypeName.INTEGER, index.getType().getSqlTypeName());
  }

  @Test
  public void bigintFieldIndexIsWrappedInCast() {
    // A non-literal BIGINT index (a field ref) cannot be constant-folded, so narrowing must
    // produce an explicit CAST to INTEGER.
    RexNode arrayRef = builder.makeInputRef(intArray(), 0);
    RexNode fieldIndex = builder.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1);
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, arrayRef, fieldIndex);

    RexCall call = (RexCall) result;
    RexNode index = call.getOperands().get(1);
    assertEquals(SqlKind.CAST, index.getKind());
    assertEquals(SqlTypeName.INTEGER, index.getType().getSqlTypeName());
  }

  @Test
  public void roundValueOperandKeepsBigint() {
    // ROUND(value, precision): value position (0) is NUMERIC ([INTEGER, DOUBLE]) and must NOT be
    // narrowed; only the precision (position 1) is int-domain.
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, ROUND, bigint(123L), bigint(1L));

    RexCall call = (RexCall) result;
    RexNode value = call.getOperands().get(0);
    // Value operand is left as BIGINT (not wrapped in a narrowing CAST).
    assertEquals(SqlTypeName.BIGINT, value.getType().getSqlTypeName());
  }

  @Test
  public void convNumberOperandKeepsBigint() {
    // CONV(number, fromBase, toBase): only the bases are Java int; the number is converted via
    // toString, so a BIGINT number such as 4294967296 must not be narrowed.
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, CONV, bigint(4294967296L), bigint(10), bigint(16));

    RexCall call = (RexCall) result;
    assertEquals(SqlTypeName.BIGINT, call.getOperands().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, call.getOperands().get(1).getType().getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, call.getOperands().get(2).getType().getSqlTypeName());
  }

  @Test
  public void timestampAddAmountKeepsBigint() {
    // TIMESTAMPADD's runtime takes a long amount, so a BIGINT amount must not be narrowed.
    RexNode timestamp = builder.makeTimestampLiteral(new TimestampString("2020-01-01 00:00:00"), 0);
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, TIMESTAMPADD, builder.makeLiteral("SECOND"), bigint(4294967296L), timestamp);

    RexCall call = (RexCall) result;
    assertEquals(SqlTypeName.BIGINT, call.getOperands().get(1).getType().getSqlTypeName());
  }

  @Test
  public void bigintMapKeyIsNotNarrowed() {
    // ITEM(map, key): only the array shape has an int index; map keys keep their type.
    RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    RelDataType mapType = TYPE_FACTORY.createMapType(bigintType, bigintType);
    RexNode mapRef = builder.makeInputRef(mapType, 0);
    RexNode key = builder.makeInputRef(bigintType, 1);
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, mapRef, key);

    RexCall call = (RexCall) result;
    assertEquals(key, call.getOperands().get(1));
  }

  @Test
  public void weekModeIsNarrowedToInteger() {
    // WEEK(date, mode): the runtime takes an int mode.
    RexNode date = builder.makeDateLiteral(new DateString("2020-01-02"));
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, WEEK, date, bigint(1));

    RexCall call = (RexCall) result;
    assertEquals(SqlTypeName.INTEGER, call.getOperands().get(1).getType().getSqlTypeName());
  }

  private RexNode string(String value) {
    return builder.makeLiteral(value);
  }

  private RexNode bigintField(int index, boolean nullable) {
    return builder.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT, nullable), index);
  }

  private static SqlTypeName operandType(RexNode call, int position) {
    return ((RexCall) call).getOperands().get(position).getType().getSqlTypeName();
  }

  // --- narrowIntArgs behavior ---

  @Test
  public void narrowingPreservesNullability() {
    RexNode arrayRef = builder.makeInputRef(intArray(), 0);
    RexNode nullableIndex = bigintField(1, true);
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, arrayRef, nullableIndex);

    RexNode index = ((RexCall) result).getOperands().get(1);
    assertEquals(SqlTypeName.INTEGER, index.getType().getSqlTypeName());
    assertTrue(index.getType().isNullable());
  }

  @Test
  public void integerArgumentIsLeftUntouched() {
    RexNode arrayRef = builder.makeInputRef(intArray(), 0);
    RexNode intIndex = builder.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, arrayRef, intIndex);

    assertSame(intIndex, ((RexCall) result).getOperands().get(1));
  }

  @Test
  public void callerArgumentArrayIsNotMutated() {
    RexNode[] args = {builder.makeInputRef(intArray(), 0), bigintField(1, false)};
    RexNode[] snapshot = args.clone();
    PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, args);

    assertArrayEquals(snapshot, args);
  }

  @Test
  public void declaredPositionBeyondArityIsIgnored() {
    // ROUND declares position 1, but the one-argument overload has no position 1.
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, ROUND, bigint(123L));

    assertEquals(SqlTypeName.BIGINT, operandType(result, 0));
  }

  // --- int-domain positions are narrowed ---

  @Test
  public void arraySliceStartAndLengthAreNarrowed() {
    RexNode arrayRef = builder.makeInputRef(intArray(), 0);
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(builder, ARRAY_SLICE, arrayRef, bigint(1), bigint(2));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 1));
    assertEquals(SqlTypeName.INTEGER, operandType(result, 2));
  }

  @Test
  public void leftLengthIsNarrowed() {
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, LEFT, string("abcdef"), bigint(2));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 1));
  }

  @Test
  public void substringStartAndLengthAreNarrowed() {
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, SUBSTRING, string("abcdef"), bigint(1), bigint(3));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 1));
    assertEquals(SqlTypeName.INTEGER, operandType(result, 2));
  }

  @Test
  public void sha2BitLengthIsNarrowed() {
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, SHA2, string("abc"), bigint(256));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 1));
  }

  @Test
  public void toNumberBaseIsNarrowed() {
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, TONUMBER, string("ff"), bigint(16));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 1));
  }

  @Test
  public void sysdatePrecisionIsNarrowed() {
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, SYSDATE, bigint(3));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 0));
  }

  @Test
  public void rexExtractGroupIndexIsNarrowed() {
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, REX_EXTRACT, string("a1"), string("(?<d>\\d)"), bigint(1));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 2));
  }

  @Test
  public void rexExtractMultiGroupIndexAndMaxMatchAreNarrowed() {
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, REX_EXTRACT_MULTI, string("a1"), string("(?<d>\\d)"), bigint(1), bigint(2));

    assertEquals(SqlTypeName.INTEGER, operandType(result, 2));
    assertEquals(SqlTypeName.INTEGER, operandType(result, 3));
  }

  // --- non-int-domain positions keep BIGINT ---

  @Test
  public void addDateDaysKeepsBigint() {
    // ADDDATE converts the day count to long at runtime.
    RexNode date = builder.makeDateLiteral(new DateString("2020-01-01"));
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, ADDDATE, date, bigint(3000000000L));

    assertEquals(SqlTypeName.BIGINT, operandType(result, 1));
  }

  @Test
  public void toSecondsInputKeepsBigint() {
    // TO_SECONDS receives its argument as an ExprValue, so BIGINT needs no narrowing.
    RexNode result = PPLFuncImpTable.INSTANCE.resolve(builder, TO_SECONDS, bigint(950501L));

    assertEquals(SqlTypeName.BIGINT, operandType(result, 0));
  }
}
