/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_ITEM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ROUND;

import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Verifies BIGINT-to-INTEGER narrowing of int-domain function arguments in {@code
 * PPLFuncImpTable#resolve} (see {@code narrowBigintArgs}). PPL widens integer arithmetic to BIGINT
 * (#5603), but operators like ITEM and ROUND take a Java {@code int} at their control positions.
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
}
