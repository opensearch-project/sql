/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.runtime.rtti.BasicSqlTypeRtti;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation.RuntimeSqlTypeName;
import org.apache.calcite.runtime.variant.VariantValue;
import org.junit.jupiter.api.Test;

/**
 * The value of a flat_object leaf as a query sees it: the value it was written with, carrying its
 * own type, and unwrapping back to that value when it reaches a row.
 */
class ExprVariantValueTest {

  private static VariantValue variant(ExprValue leaf) {
    return ExprVariantTupleValue.toVariant(leaf);
  }

  @Test
  void carriesTheLeafTypeAndUnwrapsToTheSameValue() {
    assertAll(
        () -> assertEquals("DOUBLE", variant(doubleValue(12.5)).getTypeString()),
        () -> assertEquals("VARCHAR", variant(stringValue("4")).getTypeString()),
        () -> assertEquals("INTEGER", variant(integerValue(500)).getTypeString()),
        () -> assertEquals("BIGINT", variant(longValue(7L)).getTypeString()),
        () -> assertEquals("BOOLEAN", variant(booleanValue(true)).getTypeString()),
        () -> assertNull(ExprVariantTupleValue.toVariant(nullValue())),
        () -> assertEquals(12.5, ExprVariantTupleValue.unwrap(variant(doubleValue(12.5)))),
        () -> assertEquals("4", ExprVariantTupleValue.unwrap(variant(stringValue("4")))));
  }

  // The one thing the tuple does: at the Calcite boundary every value is handed out as a variant
  // carrying the type it was written with, rather than as a plain value.
  @Test
  void valueForCalciteHandsOutTypedVariants() {
    LinkedHashMap<String, ExprValue> leaves = new LinkedHashMap<>();
    leaves.put("n", doubleValue(12.5));
    leaves.put("s", stringValue("4"));
    leaves.put("z", nullValue());
    ExprVariantTupleValue tuple = new ExprVariantTupleValue(leaves);
    Map<?, ?> forCalcite = (Map<?, ?>) tuple.valueForCalcite();
    assertAll(
        () -> assertEquals("DOUBLE", ((VariantValue) forCalcite.get("n")).getTypeString()),
        () -> assertEquals("VARCHAR", ((VariantValue) forCalcite.get("s")).getTypeString()),
        () -> assertNull(forCalcite.get("z")),
        // inside the engine it stays an ordinary tuple
        () -> assertEquals(doubleValue(12.5), tuple.tupleValue().get("n")));
  }

  // Every scalar type a value can be written as maps to a variant runtime type.
  @Test
  void everyScalarTypeMapsToARuntimeType() {
    assertAll(
        () -> assertEquals("TINYINT", variant(new ExprByteValue(1)).getTypeString()),
        () -> assertEquals("SMALLINT", variant(new ExprShortValue(1)).getTypeString()),
        () -> assertEquals("BIGINT", variant(longValue(1L)).getTypeString()),
        () -> assertEquals("REAL", variant(new ExprFloatValue(1.5f)).getTypeString()),
        () -> assertEquals("BOOLEAN", variant(booleanValue(true)).getTypeString()),
        () -> assertThrows(IllegalStateException.class, () -> variant(new ExprIpValue("1.2.3.4"))));
  }

  // A leaf written as a number and the same text are different values, as they are in _source.
  @Test
  void equalityFollowsValueAndType() {
    assertAll(
        () -> assertEquals(variant(integerValue(500)), variant(integerValue(500))),
        () -> assertNotEquals(variant(integerValue(500)), variant(stringValue("500"))),
        () -> assertNotEquals(variant(doubleValue(12.5)), variant(doubleValue(9.0))));
  }

  // A leaf renders as the text the index holds for it -- the term for 12.5 is "12.5" -- so a
  // filter evaluated here agrees with the same filter pushed down as a term query.
  @Test
  void castToTextRendersTheIndexTerm() {
    BasicSqlTypeRtti toText = new BasicSqlTypeRtti(RuntimeSqlTypeName.VARCHAR);
    assertAll(
        () -> assertEquals("503", variant(integerValue(503)).cast(toText)),
        () -> assertEquals("12.5", variant(doubleValue(12.5)).cast(toText)),
        () -> assertEquals("true", variant(booleanValue(true)).cast(toText)),
        () -> assertEquals("n/a", variant(stringValue("n/a")).cast(toText)));
  }

  // Every other target follows Calcite's own variant rule, preserved by delegation: casting to the
  // type the leaf recorded returns the value, a numeric variant converts between numeric types,
  // and a text variant cast to a number is null rather than parsed.
  @Test
  void castFollowsTheRecordedType() {
    VariantValue number = variant(integerValue(500));
    VariantValue text = variant(stringValue("500"));
    assertAll(
        () -> assertEquals(500, number.cast(new BasicSqlTypeRtti(RuntimeSqlTypeName.INTEGER))),
        () -> assertEquals(500.0, number.cast(new BasicSqlTypeRtti(RuntimeSqlTypeName.DOUBLE))),
        () -> assertNull(text.cast(new BasicSqlTypeRtti(RuntimeSqlTypeName.INTEGER))));
  }

  // An array leaf keeps its elements, each with its own type, and unwraps to a plain list.
  @Test
  void arrayVariantKeepsTypedElements() {
    VariantValue mixed =
        variant(new ExprCollectionValue(List.of(integerValue(1), stringValue("x"))));
    assertAll(
        () -> assertEquals("ARRAY", mixed.getTypeString()),
        () ->
            assertEquals(
                "INTEGER",
                ((VariantValue) ((ExprVariantValue.Array) mixed).getElements().get(0))
                    .getTypeString()),
        () -> assertEquals(List.of(1, "x"), ExprVariantTupleValue.unwrap(mixed)));
  }
}
