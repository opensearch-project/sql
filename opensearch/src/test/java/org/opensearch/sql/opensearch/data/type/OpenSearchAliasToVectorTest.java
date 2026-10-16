/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;

/**
 * An alias to a knn_vector field reports in SQL, in the V2 engine and in describe with Calcite off
 * what it reported when knn_vector mapped to ARRAY ({@code KNN_VECTOR}), and is a vector on the
 * Calcite engine.
 */
class OpenSearchAliasToVectorTest {

  private static final Map<String, Object> MAPPING =
      Map.of(
          "embedding",
          Map.of("type", "knn_vector", "dimension", 3),
          "emb_alias",
          Map.of("type", "alias", "path", "embedding"));

  private static OpenSearchAliasType alias() {
    OpenSearchDataType type =
        OpenSearchDataType.traverseAndFlatten(OpenSearchDataType.parseMapping(MAPPING))
            .get("emb_alias");
    assertInstanceOf(OpenSearchAliasType.class, type);
    return (OpenSearchAliasType) type;
  }

  @Test
  void namesAreThoseOfBefore() {
    OpenSearchAliasType alias = alias();
    assertAll(
        () -> assertEquals("KNN_VECTOR", alias.typeName()),
        () -> assertEquals("KNN_VECTOR", alias.legacyTypeName()),
        () -> assertEquals(List.of(ExprCoreType.UNKNOWN), alias.getParent()),
        () -> assertEquals("KNN_VECTOR", alias.describeTypeName(LangSpec.SQL_SPEC, false)),
        () -> assertEquals("KNN_VECTOR", alias.describeTypeName(LangSpec.SQL_SPEC, true)),
        () -> assertEquals("KNN_VECTOR", alias.describeTypeName(PPL_SPEC, false)),
        () -> assertEquals("VECTOR", alias.describeTypeName(PPL_SPEC, true)));
  }

  /** The V2 engine types a reference to the alias with the target's type, named ARRAY / NESTED. */
  @Test
  void referencesUseTheTargetType() {
    OpenSearchAliasType alias = alias();
    assertInstanceOf(OpenSearchKnnVectorType.class, alias.getOriginalExprType());
    assertEquals("ARRAY", alias.getOriginalExprType().typeName());
    assertEquals("NESTED", alias.getOriginalExprType().legacyTypeName());
  }

  @Test
  void isAVectorOnCalcite() {
    ExprVectorType type =
        (ExprVectorType) OpenSearchTypeFactory.convertExprTypeToRelDataType(alias());
    assertEquals(3, type.getDimension());
  }

  @Test
  void describeRequestNamesTheAliasAsTheMappingType() {
    OpenSearchClient client = mock(OpenSearchClient.class);
    IndexMapping mapping = mock(IndexMapping.class);
    when(mapping.getFieldMappings()).thenReturn(OpenSearchDataType.parseMapping(MAPPING));
    when(client.getIndexMappings("idx")).thenReturn(Map.of("idx", mapping));
    assertEquals("knn_vector", typeName(new OpenSearchDescribeIndexRequest(client, "idx")));
    assertEquals(
        "knn_vector", typeName(new OpenSearchDescribeIndexRequest(client, "idx", PPL_SPEC)));
    assertEquals(
        "vector", typeName(new OpenSearchDescribeIndexRequest(client, "idx", PPL_SPEC, true)));
  }

  private static String typeName(OpenSearchDescribeIndexRequest request) {
    for (ExprValue row : request.search()) {
      if (row.tupleValue().get("COLUMN_NAME").stringValue().equals("emb_alias")) {
        return row.tupleValue().get("TYPE_NAME").stringValue();
      }
    }
    throw new AssertionError("no emb_alias row");
  }
}
