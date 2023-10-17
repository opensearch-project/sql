/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchDataTypeTest {

  private static final OpenSearchDataType textType = OpenSearchDataType.of(MappingType.Text);
  private static final OpenSearchDataType textKeywordType =
      OpenSearchTextType.of(Map.of("words", OpenSearchTextType.of(MappingType.Keyword)));

  private static final String emptyFormatString = "";

  private static final OpenSearchDateType dateType = OpenSearchDateType.of(emptyFormatString);

  @Test
  public void isCompatible() {
    assertTrue(STRING.isCompatible(textType));
    assertFalse(textType.isCompatible(STRING));

    assertTrue(STRING.isCompatible(textKeywordType));
    assertTrue(textType.isCompatible(textKeywordType));
  }

  // `typeName` and `legacyTypeName` return different things:
  // https://github.com/opensearch-project/sql/issues/1296
  @Test
  public void typeName() {
    assertEquals("STRING", textType.typeName());
    assertEquals("STRING", textKeywordType.typeName());
    assertEquals("OBJECT", OpenSearchDataType.of(MappingType.Object).typeName());
    assertEquals("DATE", OpenSearchDataType.of(MappingType.Date).typeName());
    assertEquals("DOUBLE", OpenSearchDataType.of(MappingType.Double).typeName());
    assertEquals("KEYWORD", OpenSearchDataType.of(MappingType.Keyword).typeName());
  }

  @Test
  public void legacyTypeName() {
    assertEquals("TEXT", textType.legacyTypeName());
    assertEquals("TEXT", textKeywordType.legacyTypeName());
    assertEquals("OBJECT", OpenSearchDataType.of(MappingType.Object).legacyTypeName());
    assertEquals("DATE", OpenSearchDataType.of(MappingType.Date).legacyTypeName());
    assertEquals("DOUBLE", OpenSearchDataType.of(MappingType.Double).legacyTypeName());
    assertEquals("KEYWORD", OpenSearchDataType.of(MappingType.Keyword).legacyTypeName());
  }

  @Test
  public void shouldCast() {
    assertFalse(textType.shouldCast(STRING));
    assertFalse(textKeywordType.shouldCast(STRING));
  }

  private static Stream<Arguments> getTestDataWithType() {
    return Stream.of(
        Arguments.of(MappingType.Text, "text", OpenSearchTextType.of()),
        Arguments.of(MappingType.Keyword, "keyword", STRING),
        Arguments.of(MappingType.Byte, "byte", BYTE),
        Arguments.of(MappingType.Short, "short", SHORT),
        Arguments.of(MappingType.Integer, "integer", INTEGER),
        Arguments.of(MappingType.Long, "long", LONG),
        Arguments.of(MappingType.HalfFloat, "half_float", FLOAT),
        Arguments.of(MappingType.Float, "float", FLOAT),
        Arguments.of(MappingType.ScaledFloat, "scaled_float", DOUBLE),
        Arguments.of(MappingType.Double, "double", DOUBLE),
        Arguments.of(MappingType.Boolean, "boolean", BOOLEAN),
        Arguments.of(MappingType.Date, "date", TIMESTAMP),
        Arguments.of(MappingType.DateNanos, "date", TIMESTAMP),
        Arguments.of(MappingType.Object, "object", STRUCT),
        Arguments.of(MappingType.Nested, "nested", ARRAY),
        Arguments.of(MappingType.GeoPoint, "geo_point", OpenSearchGeoPointType.of()),
        Arguments.of(MappingType.Binary, "binary", OpenSearchBinaryType.of()),
        Arguments.of(MappingType.Ip, "ip", OpenSearchIpType.of()));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("getTestDataWithType")
  public void of_MappingType(MappingType mappingType, String name, ExprType dataType) {
    var type = OpenSearchDataType.of(mappingType);
    // For serialization of SQL and PPL different functions are used, and it was designed to return
    // different types. No clue why, but it should be fixed in #1296.
    var nameForSQL = name.toUpperCase();
    var nameForPPL = name.equals("text") ? "STRING" : name.toUpperCase();
    assertAll(
        () -> assertEquals(nameForPPL, type.typeName()),
        () -> assertEquals(nameForSQL, type.legacyTypeName()),
        () -> assertEquals(dataType, type.getExprType()));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(ExprCoreType.class)
  public void of_ExprCoreType(ExprCoreType coreType) {
    assumeFalse(coreType == UNKNOWN);
    var type = OpenSearchDataType.of(coreType);
    if (type instanceof OpenSearchDateType) {
      assertEquals(coreType, type.getExprType());
    } else {
      assertEquals(coreType.toString(), type.typeName());
      assertEquals(coreType.toString(), type.legacyTypeName());
      assertEquals(coreType, type.getExprType());
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(ExprCoreType.class)
  public void of_OpenSearchDataType_from_ExprCoreType(ExprCoreType coreType) {
    var type = OpenSearchDataType.of(coreType);
    var derivedType = OpenSearchDataType.of(type);
    assertEquals(type, derivedType);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(MappingType.class)
  public void of_OpenSearchDataType_from_MappingType(OpenSearchDataType.MappingType mappingType) {
    assumeFalse(mappingType == MappingType.Invalid);
    var type = OpenSearchDataType.of(mappingType);
    var derivedType = OpenSearchDataType.of(type);
    assertEquals(type, derivedType);
  }

  @Test
  // All types without `fields` and `properties` are singletones unless cloned.
  public void types_but_clones_are_singletons_and_cached() {
    var type = OpenSearchDataType.of(MappingType.Object);
    var alsoType = OpenSearchDataType.of(MappingType.Object);
    Map<String, Object> properties =
        Map.of("properties", Map.of("number", Map.of("type", "integer")));
    var typeWithProperties = OpenSearchDataType.of(MappingType.Object, properties);
    var typeWithFields = OpenSearchDataType.of(MappingType.Text, Map.of());
    var cloneType = type.cloneEmpty();

    assertAll(
        () -> assertSame(type, alsoType),
        () -> assertNotSame(type, cloneType),
        () -> assertNotSame(type, typeWithProperties),
        () -> assertNotSame(type, typeWithFields),
        () -> assertNotSame(typeWithProperties, typeWithProperties.cloneEmpty()),
        () -> assertNotSame(typeWithFields, typeWithFields.cloneEmpty()),
        () -> assertNotSame(dateType, dateType.cloneEmpty()),
        () -> assertSame(OpenSearchDataType.of(MappingType.Text), OpenSearchTextType.of()),
        () -> assertSame(OpenSearchDataType.of(MappingType.Binary), OpenSearchBinaryType.of()),
        () -> assertSame(OpenSearchDataType.of(MappingType.GeoPoint), OpenSearchGeoPointType.of()),
        () -> assertSame(OpenSearchDataType.of(MappingType.Ip), OpenSearchIpType.of()),
        () ->
            assertNotSame(
                OpenSearchTextType.of(),
                OpenSearchTextType.of(Map.of("properties", OpenSearchDataType.of(INTEGER)))),
        () -> assertSame(OpenSearchDataType.of(INTEGER), OpenSearchDataType.of(INTEGER)),
        () -> assertSame(OpenSearchDataType.of(STRING), OpenSearchDataType.of(STRING)),
        () -> assertSame(OpenSearchDataType.of(STRUCT), OpenSearchDataType.of(STRUCT)),
        () ->
            assertNotSame(
                OpenSearchDataType.of(INTEGER), OpenSearchDataType.of(INTEGER).cloneEmpty()));
  }

  @Test
  // Use OpenSearchDataType.of(type, properties, fields) or OpenSearchTextType.of(fields)
  // to create a new type object with required parameters. Types are immutable, even clones.
  public void fields_and_properties_are_readonly() {
    var objectType = OpenSearchDataType.of(MappingType.Object);
    var textType = OpenSearchTextType.of();
    var textTypeWithFields =
        OpenSearchTextType.of(Map.of("letters", OpenSearchDataType.of(MappingType.Keyword)));
    assertAll(
        () ->
            assertThrows(
                UnsupportedOperationException.class,
                () -> objectType.getProperties().put("something", OpenSearchDataType.of(INTEGER))),
        () ->
            assertThrows(
                UnsupportedOperationException.class,
                () ->
                    textType.getFields().put("words", OpenSearchDataType.of(MappingType.Keyword))),
        () ->
            assertThrows(
                UnsupportedOperationException.class,
                () ->
                    textTypeWithFields
                        .getFields()
                        .put("words", OpenSearchDataType.of(MappingType.Keyword))));
  }

  @Test
  // Test and type added for coverage only
  public void of_null_MappingType() {
    assertNotNull(OpenSearchDataType.of(MappingType.Invalid));
  }

  @Test
  // cloneEmpty doesn't clone properties and fields.
  // Fields are cloned by OpenSearchTextType::cloneEmpty, because it is used in that type only.
  public void cloneEmpty() {
    var type =
        OpenSearchDataType.of(MappingType.Object, Map.of("val", OpenSearchDataType.of(INTEGER)));
    var clone = type.cloneEmpty();
    var textClone = textKeywordType.cloneEmpty();

    assertAll(
        // can compare because `properties` and `fields` are marked as @EqualsAndHashCode.Exclude
        () -> assertEquals(type, clone),
        () -> assertTrue(clone.getProperties().isEmpty()),
        () -> assertEquals(textKeywordType, textClone),
        () ->
            assertEquals(
                FieldUtils.readField(textKeywordType, "fields", true),
                FieldUtils.readField(textClone, "fields", true)));
  }

  // Following structure of nested objects should be flattened
  // =====================
  // type
  //    |- subtype
  //    |        |
  //    |        |- subsubtype
  //    |        |           |
  //    |        |           |- textWithKeywordType
  //    |        |           |                    |- keyword
  //    |        |           |
  //    |        |           |- INTEGER
  //    |        |
  //    |        |- GeoPoint
  //    |        |- textWithFieldsType
  //    |                            |- words
  //    |
  //    |- text
  //    |- keyword
  // =================
  // as
  // =================
  // mapping : Object
  // mapping.submapping : Object
  // mapping.submapping.subsubmapping : Object
  // mapping.submapping.subsubmapping.textWithKeywordType : Text
  //                                           |- keyword : Keyword
  // mapping.submapping.subsubmapping.INTEGER : INTEGER
  // mapping.submapping.geo_point : GeoPoint
  // mapping.submapping.textWithFieldsType: Text
  //                               |- words : Keyword
  // mapping.text : Text
  // mapping.keyword : Keyword
  // ==================
  // Objects are flattened by OpenSearch, but Texts aren't
  // TODO Arrays
  @Test
  public void traverseAndFlatten() {
    var flattened = OpenSearchDataType.traverseAndFlatten(getSampleMapping());
    var objectType = OpenSearchDataType.of(MappingType.Object);
    assertAll(
        () -> assertEquals(11, flattened.size()),
        () -> assertTrue(flattened.get("mapping").getProperties().isEmpty()),
        () -> assertTrue(flattened.get("mapping.submapping").getProperties().isEmpty()),
        () ->
            assertTrue(flattened.get("mapping.submapping.subsubmapping").getProperties().isEmpty()),
        () -> assertEquals(objectType, flattened.get("mapping")),
        () -> assertEquals(objectType, flattened.get("mapping.submapping")),
        () -> assertEquals(objectType, flattened.get("mapping.submapping.subsubmapping")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Keyword), flattened.get("mapping.keyword")),
        () -> assertEquals(OpenSearchDataType.of(MappingType.Text), flattened.get("mapping.text")),
        () ->
            assertEquals(
                OpenSearchGeoPointType.of(), flattened.get("mapping.submapping.geo_point")),
        () ->
            assertEquals(
                OpenSearchTextType.of(), flattened.get("mapping.submapping.textWithFieldsType")),
        () ->
            assertEquals(
                OpenSearchTextType.of(),
                flattened.get("mapping.submapping.subsubmapping.texttype")),
        () ->
            assertEquals(
                OpenSearchDataType.of(INTEGER),
                flattened.get("mapping.submapping.subsubmapping.INTEGER")));
  }

  @Test
  public void resolve() {
    var mapping = getSampleMapping();

    assertAll(
        () -> assertNull(OpenSearchDataType.resolve(mapping, "incorrect")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Object),
                OpenSearchDataType.resolve(mapping, "mapping")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Object),
                OpenSearchDataType.resolve(mapping, "submapping")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Object),
                OpenSearchDataType.resolve(mapping, "subsubmapping")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Text),
                OpenSearchDataType.resolve(mapping, "texttype")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Text),
                OpenSearchDataType.resolve(mapping, "textWithFieldsType")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Text),
                OpenSearchDataType.resolve(mapping, "text")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Integer),
                OpenSearchDataType.resolve(mapping, "INTEGER")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.GeoPoint),
                OpenSearchDataType.resolve(mapping, "geo_point")),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Keyword),
                OpenSearchDataType.resolve(mapping, "keyword")));
  }

  // type : Object
  // type.subtype : Object
  // type.subtype.subsubtype : Object
  // type.subtype.subsubtype.textWithKeywordType : Text
  //                                           |- keyword : Keyword
  // type.subtype.subsubtype.INTEGER : INTEGER
  // type.subtype.geo_point : GeoPoint
  // type.subtype.textWithFieldsType: Text
  //                               |- words : Keyword
  // type.text : Text
  // type.keyword : Keyword

  @Test
  public void text_type_with_fields_ctor() {
    var type = OpenSearchTextType.of(Map.of("words", OpenSearchDataType.of(MappingType.Keyword)));
    assertAll(
        () -> assertEquals(OpenSearchTextType.of(), type),
        () -> assertEquals(1, type.getFields().size()),
        () ->
            assertEquals(
                OpenSearchDataType.of(MappingType.Keyword), type.getFields().get("words")));
  }

  private Map<String, OpenSearchDataType> getSampleMapping() {
    Map<String, Object> subsubmapping =
        Map.of(
            "properties",
            Map.of(
                "texttype", Map.of("type", "text"),
                "INTEGER", Map.of("type", "integer")));

    Map<String, Object> submapping =
        Map.of(
            "properties",
            Map.of(
                "subsubmapping", subsubmapping,
                "textWithFieldsType", Map.of("type", "text", "fieldsType", true),
                "geo_point", Map.of("type", "geo_point")));

    Map<String, Object> types =
        Map.of(
            "properties",
            Map.of(
                "submapping", submapping,
                "keyword", Map.of("type", "keyword"),
                "text", Map.of("type", "text")));

    var mapping = OpenSearchDataType.of(MappingType.Object, types);
    return Map.of("mapping", mapping);
  }

  @Test
  public void test_getExprType() {
    assertEquals(OpenSearchTextType.of(), OpenSearchDataType.of(MappingType.Text).getExprType());
    assertEquals(FLOAT, OpenSearchDataType.of(MappingType.Float).getExprType());
    assertEquals(FLOAT, OpenSearchDataType.of(MappingType.HalfFloat).getExprType());
    assertEquals(DOUBLE, OpenSearchDataType.of(MappingType.Double).getExprType());
    assertEquals(DOUBLE, OpenSearchDataType.of(MappingType.ScaledFloat).getExprType());
    assertEquals(TIMESTAMP, OpenSearchDataType.of(MappingType.Date).getExprType());
  }

  @Test
  public void test_shouldCastFunction() {
    assertFalse(dateType.shouldCast(DATE));
  }
}
