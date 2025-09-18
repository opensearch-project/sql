/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.JsonBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExtendedRelJsonTest {
  private final ExtendedRelJson relJson = ExtendedRelJson.create(new JsonBuilder());
  private final OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;

  @Test
  void testSerializeSqlType() {
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType integerType = typeFactory.createSqlType(SqlTypeName.INTEGER, true);
    RelDataType decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 4);

    assertEquals(
        Map.of("type", "VARCHAR", "nullable", false, "precision", -1), relJson.toJson(varcharType));
    assertEquals(Map.of("type", "INTEGER", "nullable", true), relJson.toJson(integerType));
    assertEquals(
        Map.of("type", "DECIMAL", "nullable", false, "precision", 4, "scale", 4),
        relJson.toJson(decimalType));
  }

  @Test
  void testSerializeUDT() {
    RelDataType dateType = typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_DATE);
    RelDataType timeType = typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIME, true);
    RelDataType timestampType = typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP);

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
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 4),
        relJson.toType(typeFactory, serializedDecimal));
  }

  @Test
  void testDeserializeUDT() {
    Map<String, Object> serializedTimestamp =
        Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", true, "precision", -1);
    assertEquals(
        typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP, true).toString(),
        relJson.toType(typeFactory, serializedTimestamp).toString());
  }

  @Test
  void testSerializeRelDataTypeField() {
    RelDataType structType =
        typeFactory
            .builder()
            .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("timestamp", typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP))
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
        typeFactory
            .builder()
            .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("timestamp", typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP))
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
    RelDataType resultType = relJson.toType(typeFactory, structMap);

    assertEquals(resultType, expectedType);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testSerializeArrayTypes() {
    RelDataType stringArrayType =
        typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);

    RelDataType timestampArrayType =
        typeFactory.createArrayType(
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP), -1);

    RelDataType ipArrayType =
        typeFactory.createArrayType(
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_IP), -1);

    assertEquals(
        Map.of(
            "type",
            "ARRAY",
            "nullable",
            false,
            "component",
            Map.of("type", "VARCHAR", "nullable", false, "precision", -1)),
        relJson.toJson(stringArrayType));

    assertEquals(
        Map.of(
            "type",
            "ARRAY",
            "nullable",
            false,
            "component",
            Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", false, "precision", -1)),
        relJson.toJson(timestampArrayType));

    Object serializedIpArray = relJson.toJson(ipArrayType);
    Map<String, Object> serializedMap = (Map<String, Object>) serializedIpArray;
    assertEquals("ARRAY", serializedMap.get("type"));
    assertEquals(false, serializedMap.get("nullable"));
    assertInstanceOf(Map.class, serializedMap.get("component"));
    Map<String, Object> componentMap = (Map<String, Object>) serializedMap.get("component");
    assertEquals("EXPR_IP", componentMap.get("udt"));
  }

  @Test
  void testDeserializeArrayTypes() {
    Map<String, Object> serializedTimestampArray =
        Map.of(
            "type",
            "ARRAY",
            "nullable",
            false,
            "component",
            Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", false, "precision", -1));

    RelDataType expectedTimestampArray =
        typeFactory.createArrayType(
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP), -1);

    RelDataType deserializedType = relJson.toType(typeFactory, serializedTimestampArray);
    assertEquals(expectedTimestampArray, deserializedType);

    assertEquals(
        typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
        deserializedType.getComponentType());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testSerializeMapTypes() {
    RelDataType regularMapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            false);

    RelDataType mapWithUdtValueType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
            true);

    RelDataType complexMapType =
        typeFactory.createMapType(
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_IP),
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
            false);

    Map<String, Object> expectedRegularMap =
        Map.of(
            "type",
            "MAP",
            "nullable",
            false,
            "key",
            Map.of("type", "VARCHAR", "nullable", false, "precision", -1),
            "value",
            Map.of("type", "INTEGER", "nullable", false));
    assertEquals(expectedRegularMap, relJson.toJson(regularMapType));

    Map<String, Object> expectedUdtValueMap =
        Map.of(
            "type",
            "MAP",
            "nullable",
            true,
            "key",
            Map.of("type", "VARCHAR", "nullable", false, "precision", -1),
            "value",
            Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", false, "precision", -1));
    assertEquals(expectedUdtValueMap, relJson.toJson(mapWithUdtValueType));

    Object serializedComplexMap = relJson.toJson(complexMapType);
    assertInstanceOf(Map.class, serializedComplexMap);
    Map<String, Object> serializedMap = (Map<String, Object>) serializedComplexMap;

    assertEquals("MAP", serializedMap.get("type"));
    assertEquals(false, serializedMap.get("nullable"));

    assertInstanceOf(Map.class, serializedMap.get("key"));
    Map<String, Object> keyMap = (Map<String, Object>) serializedMap.get("key");
    assertEquals("EXPR_IP", keyMap.get("udt"));

    assertInstanceOf(Map.class, serializedMap.get("value"));
    Map<String, Object> valueMap = (Map<String, Object>) serializedMap.get("value");
    assertEquals("EXPR_TIMESTAMP", valueMap.get("udt"));
    assertEquals("VARCHAR", valueMap.get("type"));
  }

  @Test
  void testDeserializeMapTypes() {
    Map<String, Object> serializedComplexMap =
        Map.of(
            "type",
            "MAP",
            "nullable",
            false,
            "key",
            Map.of("udt", "EXPR_IP", "type", "VARCHAR", "nullable", false, "precision", -1),
            "value",
            Map.of("udt", "EXPR_TIMESTAMP", "type", "VARCHAR", "nullable", false, "precision", -1));

    RelDataType expectedComplexMap =
        typeFactory.createMapType(
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_IP),
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
            false);

    RelDataType deserializedType = relJson.toType(typeFactory, serializedComplexMap);
    assertEquals(expectedComplexMap, deserializedType);

    assertEquals(
        typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_IP),
        deserializedType.getKeyType());
    assertEquals(
        typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
        deserializedType.getValueType());
  }

  @Test
  void testSerializeAndDeserializeNestedStructure() {
    RelDataType innerMapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
            false);
    RelDataType complexType = typeFactory.createArrayType(innerMapType, -1);

    Object serialized = relJson.toJson(complexType);
    RelDataType deserialized = relJson.toType(typeFactory, serialized);

    assertEquals(complexType, deserialized);

    assertEquals(
        SqlTypeName.MAP, Objects.requireNonNull(deserialized.getComponentType()).getSqlTypeName());
    assertEquals(
        SqlTypeName.VARCHAR,
        Objects.requireNonNull(deserialized.getComponentType().getKeyType()).getSqlTypeName());
    assertEquals(
        typeFactory.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP),
        deserialized.getComponentType().getValueType());
  }
}
