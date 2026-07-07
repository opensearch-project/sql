/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

class ExtendedRexBuilderTest {

  private static final RexBuilder REX_BUILDER =
      new ExtendedRexBuilder(new RexBuilder(TYPE_FACTORY));

  /**
   * VARCHAR → VARBINARY casts must be rewritten as a {@code BINARY(varchar)} placeholder {@code
   * RexCall}.
   */
  @Test
  void castVarcharToVarbinaryEmitsBinaryPlaceholder() {
    // Use makeInputRef to construct a VARCHAR-typed RexNode reliably. makeLiteral(String) folds
    // to CHAR, which would make this test pass-through default cast instead of our placeholder.
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RexNode varcharRef = REX_BUILDER.makeInputRef(varchar, 0);
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);

    RexNode result = REX_BUILDER.makeCast(varbinary, varcharRef);

    assertInstanceOf(RexCall.class, result);
    RexCall call = (RexCall) result;
    assertEquals(PPLBuiltinOperators.BINARY, call.getOperator());
    assertEquals("BINARY", call.getOperator().getName());
    assertEquals(SqlTypeName.VARBINARY, call.getType().getSqlTypeName());
    assertEquals(1, call.getOperands().size());
    assertEquals(SqlTypeName.VARCHAR, call.getOperands().get(0).getType().getSqlTypeName());
  }

  /**
   * Casts targeting a SqlTypeName other than VARBINARY must NOT trigger the BINARY rewrite — they
   * fall through to Calcite's default cast handling.
   */
  @Test
  void castVarcharToIntegerDoesNotEmitBinaryPlaceholder() {
    RexNode varcharLiteral = REX_BUILDER.makeLiteral("42");
    RelDataType integer = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RexNode result = REX_BUILDER.makeCast(integer, varcharLiteral);

    assertNotNull(result);
    if (result instanceof RexCall call) {
      assertEquals(
          "BINARY".equals(call.getOperator().getName()),
          false,
          "VARCHAR → INTEGER must not emit BINARY placeholder");
    }
  }

  /**
   * Casts whose source is not VARCHAR must also fall through. The placeholder is only meant for the
   * (VARCHAR → VARBINARY) case where a string IP / base64 literal is being compared against a
   * VARBINARY column — non-string sources have well-defined Calcite cast semantics that should not
   * be hijacked.
   */
  @Test
  void castIntegerToVarbinaryDoesNotEmitBinaryPlaceholder() {
    RexNode intLiteral = REX_BUILDER.makeExactLiteral(java.math.BigDecimal.ONE);
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);

    RexNode result = REX_BUILDER.makeCast(varbinary, intLiteral);

    assertNotNull(result);
    if (result instanceof RexCall call) {
      assertEquals(
          "BINARY".equals(call.getOperator().getName()),
          false,
          "non-VARCHAR → VARBINARY must not emit BINARY placeholder");
    }
  }
}
