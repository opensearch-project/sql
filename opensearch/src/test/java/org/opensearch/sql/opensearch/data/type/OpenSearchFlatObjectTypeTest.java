/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

class OpenSearchFlatObjectTypeTest {

  @Test
  void of_isASingletonThatSurvivesCloning() {
    OpenSearchFlatObjectType type = OpenSearchFlatObjectType.of();
    assertAll(
        () -> assertSame(type, OpenSearchFlatObjectType.of()),
        () -> assertSame(type, OpenSearchDataType.of(OpenSearchDataType.MappingType.FlatObject)),
        () -> assertSame(type, type.cloneEmpty()),
        () -> assertSame(type, type.cloneDeep()));
  }

  // Like text, geo_point and binary: no core-type counterpart, so the type stands for itself in
  // the type environment and is mapped to Calcite by name.
  @Test
  void hasNoCoreTypeAndResolvesToItself() {
    OpenSearchFlatObjectType type = OpenSearchFlatObjectType.of();
    assertAll(
        () -> assertEquals(UNKNOWN, type.getExprCoreType()),
        () -> assertSame(type, type.getExprType()),
        () -> assertEquals("FLAT_OBJECT", type.legacyTypeName()),
        () -> assertEquals(OpenSearchDataType.MappingType.FlatObject, type.getMappingType()),
        () -> assertTrue(type.getProperties().isEmpty()));
  }

  // The field is presented to the planner as a map from dotted leaf path to a VARIANT value, so
  // each leaf keeps the type it was written with.
  @Test
  void mapsToCalciteMapOfVarcharToVariant() {
    RelDataType relType =
        OpenSearchTypeFactory.convertExprTypeToRelDataType(OpenSearchFlatObjectType.of());
    assertAll(
        () -> assertEquals(SqlTypeName.MAP, relType.getSqlTypeName()),
        () -> assertEquals(SqlTypeName.VARCHAR, relType.getKeyType().getSqlTypeName()),
        () -> assertEquals(SqlTypeName.VARIANT, relType.getValueType().getSqlTypeName()),
        () -> assertTrue(relType.getValueType().isNullable()));
  }

  // The parsed mapping keeps the singleton, so a schema built from it uses the map type above.
  @Test
  void parsedMappingUsesTheSingleton() {
    Map<String, OpenSearchDataType> parsed =
        OpenSearchDataType.parseMapping(Map.of("attributes", Map.of("type", "flat_object")));
    assertSame(OpenSearchFlatObjectType.of(), parsed.get("attributes"));
  }
}
