/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.JsonBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExtendedRelJsonTest {
  private final ExtendedRelJson relJson = ExtendedRelJson.create(new JsonBuilder());

  @Test
  void testSerializeSqlType() {
    RelDataType varcharType = OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType integerType =
        OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, true);
    RelDataType decimalType =
        OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 4, 4);

    assertEquals(
        Map.of("type", "VARCHAR", "nullable", false, "precision", -1), relJson.toJson(varcharType));
    assertEquals(Map.of("type", "INTEGER", "nullable", true), relJson.toJson(integerType));
    assertEquals(
        Map.of("type", "DECIMAL", "nullable", false, "precision", 4, "scale", 4),
        relJson.toJson(decimalType));
  }

  @Test
  void testSerializeUDT() {
    RelDataType dateType =
        OpenSearchTypeFactory.TYPE_FACTORY.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_DATE);
    RelDataType timeType =
        OpenSearchTypeFactory.TYPE_FACTORY.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIME, true);
    RelDataType timestampType =
        OpenSearchTypeFactory.TYPE_FACTORY.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP);

    assertEquals(
        Map.of("udt", "EXPR_DATE", "type", "VARCHAR", "nullable", false, "precision", -1),
        relJson.toJson(dateType));
    assertEquals(
        Map.of("udt", "EXPR_TIME", "type", "VARCHAR", "nullable", true, "precision", -1),
        relJson.toJson(timeType));
    assertEquals(
        Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", false, "precision", -1),
        relJson.toJson(timestampType));
  }

  @Test
  void testDeserializeSqlType() {
    Map<String, Object> serializedDecimal =
        Map.of("type", "DECIMAL", "nullable", false, "precision", 4, "scale", 4);
    assertEquals(
        OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 4, 4),
        relJson.toType(OpenSearchTypeFactory.TYPE_FACTORY, serializedDecimal));
  }

  @Test
  void testDeserializeUDT() {
    Map<String, Object> serializedTimestamp =
        Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", true, "precision", -1);
    assertEquals(
        OpenSearchTypeFactory.TYPE_FACTORY
            .createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP, true)
            .toString(),
        relJson.toType(OpenSearchTypeFactory.TYPE_FACTORY, serializedTimestamp).toString());
  }

  @Test
  void testSerializeRelDataTypeField() {
    RelDataType structType =
        OpenSearchTypeFactory.TYPE_FACTORY
            .builder()
            .add("name", OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            .add(
                "timestamp",
                OpenSearchTypeFactory.TYPE_FACTORY.createUDT(
                    OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP))
            .build();

    RelDataTypeField nameField = structType.getFieldList().get(0);
    RelDataTypeField timestampField = structType.getFieldList().get(1);

    // Test serialization of regular field
    Object nameFieldJson = relJson.toJson((Object) nameField);
    assertEquals(
        Map.of("type", "VARCHAR", "nullable", false, "precision", -1, "name", "name"),
        nameFieldJson);

    // Test serialization of UDT field
    Object timestampFieldJson = relJson.toJson(timestampField);
    assertEquals(
        Map.of(
            "udt",
            "EXPR_TIMESTAMP",
            "type",
            "VARCHAR",
            "nullable",
            false,
            "precision",
            -1,
            "name",
            "timestamp"),
        timestampFieldJson);
  }

  @Test
  void testDeserializeRelDataTypeField() {
    RelDataType expectedType =
        OpenSearchTypeFactory.TYPE_FACTORY
            .builder()
            .add("name", OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            .add(
                "timestamp",
                OpenSearchTypeFactory.TYPE_FACTORY.createUDT(
                    OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP))
            .build();

    Map<String, Object> nameFieldMap =
        Map.of("type", "VARCHAR", "nullable", false, "precision", -1, "name", "name");
    Map<String, Object> udtFieldMap =
        Map.of(
            "udt",
            "EXPR_TIMESTAMP",
            "type",
            "VARCHAR",
            "nullable",
            false,
            "precision",
            -1,
            "name",
            "timestamp");
    Map<String, Object> structMap =
        Map.of(
            "fields",
            java.util.Arrays.asList(nameFieldMap, udtFieldMap),
            "type",
            "struct",
            "nullable",
            false);
    RelDataType resultType = relJson.toType(OpenSearchTypeFactory.TYPE_FACTORY, structMap);

    assertEquals(resultType.toString(), expectedType.toString());
  }
}
