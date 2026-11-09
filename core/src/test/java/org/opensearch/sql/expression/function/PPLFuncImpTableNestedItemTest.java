/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_ITEM;

import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Return-type behavior of {@code ITEM} (the internal {@code item} builtin behind {@code
 * events.name}, {@code arr[0]}, {@code map['k']}) — specifically the nested-field case where a
 * {@code nested} mapping is exposed as {@code ARRAY<ROW<...>>}. Stock Calcite types {@code
 * ITEM(ARRAY<ROW>, 'field')} as the whole {@code ROW}; the override resolves it to the named
 * field's type instead. Array-index and map-key access must be left untouched.
 */
public class PPLFuncImpTableNestedItemTest {

  private final RexBuilder builder = new RexBuilder(TYPE_FACTORY);

  /** ARRAY&lt;ROW&lt;name:VARCHAR, count:INTEGER&gt;&gt; — how a nested mapping is exposed. */
  private RelDataType structArray() {
    RelDataType row =
        TYPE_FACTORY
            .builder()
            .add("name", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            .add("count", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))
            .build();
    return TYPE_FACTORY.createArrayType(row, -1);
  }

  private RexNode item(RelDataType arrayType, RexNode key) {
    RexNode arrayRef = builder.makeInputRef(arrayType, 0);
    return PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, arrayRef, key);
  }

  @Test
  public void keywordLeafIsTypedAsTheFieldNotTheRow() {
    RexNode result = item(structArray(), builder.makeLiteral("name"));
    assertEquals(SqlTypeName.VARCHAR, result.getType().getSqlTypeName());
    assertFalse(result.getType().isStruct(), "must be the leaf field, not the whole ROW");
    assertTrue(result.getType().isNullable(), "an empty array yields NULL");
  }

  @Test
  public void numericLeafIsTypedAsTheField() {
    RexNode result = item(structArray(), builder.makeLiteral("count"));
    assertEquals(SqlTypeName.INTEGER, result.getType().getSqlTypeName());
    assertFalse(result.getType().isStruct());
  }

  @Test
  public void unknownFieldFallsBackToStockRowTyping() {
    // No such field in the ROW: the override bails and defers to stock Calcite ITEM, which types
    // the
    // call as the array's component (the whole ROW) — behavior we deliberately do not change.
    RexNode result = item(structArray(), builder.makeLiteral("missing"));
    assertTrue(result.getType().isStruct(), "unknown field falls back to the whole ROW");
  }

  @Test
  public void arrayIndexAccessIsUnaffected() {
    // ITEM(ARRAY<INTEGER>, 1) is (ARRAY, INTEGER) — not (ARRAY, CHARACTER) — so it never enters the
    // override and keeps returning the element type.
    RelDataType intArray =
        TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), -1);
    RexNode idx =
        builder.makeExactLiteral(BigDecimal.ONE, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    RexNode result = item(intArray, idx);
    assertEquals(SqlTypeName.INTEGER, result.getType().getSqlTypeName());
  }

  @Test
  public void mapKeyAccessIsUnaffected() {
    // ITEM(MAP<VARCHAR,INTEGER>, 'k') is (MAP, *) — not (ARRAY, *) — so the override's ARRAY guard
    // skips it and it keeps returning the map value type.
    RelDataType mapType =
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    RexNode mapRef = builder.makeInputRef(mapType, 0);
    RexNode result =
        PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, mapRef, builder.makeLiteral("k"));
    assertEquals(SqlTypeName.INTEGER, result.getType().getSqlTypeName());
  }

  @Test
  public void stringKeyOnNonStructArrayFallsThrough() {
    // (ARRAY, CHARACTER) matches the override's signature, but the component is not a ROW — the
    // isStruct guard fails, so it defers to stock ITEM (no field lookup, type is the element type).
    RelDataType stringArray =
        TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), -1);
    RexNode result = item(stringArray, builder.makeLiteral("x"));
    assertEquals(SqlTypeName.VARCHAR, result.getType().getSqlTypeName());
    assertFalse(result.getType().isStruct());
  }
}
