/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;

class FormatPlannerFieldTypeTest {

  private JavaTypeFactoryImpl typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl();
  }

  @Test
  void acceptsScalarAndMultivalueTypesThatCanBeConvertedToText() {
    RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType integerArray = typeFactory.createArrayType(integer, -1);
    RelDataType integerMultiset = typeFactory.createMultisetType(integer, -1);

    assertDoesNotThrow(
        () -> FormatPlanner.validateFieldType("status", integer, false, typeFactory));
    assertDoesNotThrow(
        () -> FormatPlanner.validateFieldType("statuses", integerArray, true, typeFactory));
    assertDoesNotThrow(
        () -> FormatPlanner.validateFieldType("status_set", integerMultiset, false, typeFactory));
  }

  @Test
  void explicitFormatIdentifiesUnsupportedObjectField() {
    RelDataType objectType = typeFactory.builder().add("status", SqlTypeName.INTEGER).build();

    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> FormatPlanner.validateFieldType("payload", objectType, false, typeFactory));

    assertEquals(
        "The format command cannot convert field 'payload' of type OBJECT to text. Select scalar"
            + " fields before format.",
        exception.getMessage());
  }

  @Test
  void implicitFormatIdentifiesUnsupportedObjectArrayField() {
    RelDataType objectType = typeFactory.builder().add("status", SqlTypeName.INTEGER).build();
    RelDataType objectArray = typeFactory.createArrayType(objectType, -1);

    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> FormatPlanner.validateFieldType("payloads", objectArray, true, typeFactory));

    assertEquals(
        "The subsearch cannot use field 'payloads' of type ARRAY<OBJECT> in a search predicate."
            + " Return scalar fields from the subsearch instead.",
        exception.getMessage());
  }

  @Test
  void explicitFormatIdentifiesUnsupportedMapField() {
    RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.INTEGER));

    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> FormatPlanner.validateFieldType("attributes", mapType, false, typeFactory));

    assertEquals(
        "The format command cannot convert field 'attributes' of type MAP<VARCHAR, INTEGER> to"
            + " text. Select scalar fields before format.",
        exception.getMessage());
  }
}
