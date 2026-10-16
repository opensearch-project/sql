/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;

@ExtendWith(MockitoExtension.class)
class OpenSearchDescribeIndexRequestTest {

  @Mock private OpenSearchClient client;

  @Mock private IndexMapping mapping;

  @Mock private IndexMapping mapping2;

  /**
   * Merging must not mutate the mappings it reads: {@code MergeRuleHelper} rewrites the accumulated
   * type's nested {@code properties} in place, so without a copy the first index's nested field
   * would be merged into the second's and a fetched mapping would no longer describe its index.
   */
  @Test
  void getFieldTypesLeavesFetchedMappingsIntact() {
    Map<String, OpenSearchDataType> keywordSide =
        Map.of("attrs", nestedObject("env", OpenSearchDataType.MappingType.Keyword));
    Map<String, OpenSearchDataType> textSide =
        Map.of("attrs", nestedObject("env", OpenSearchDataType.MappingType.Text));
    when(mapping.getFieldMappings()).thenReturn(keywordSide);
    when(mapping2.getFieldMappings()).thenReturn(textSide);
    when(client.getIndexMappings("idx-*"))
        .thenReturn(ImmutableMap.of("idx-keyword", mapping, "idx-text", mapping2));

    OpenSearchDescribeIndexRequest request = new OpenSearchDescribeIndexRequest(client, "idx-*");
    request.getFieldTypes();

    // Each fetched mapping must still declare the type it came with.
    assertEquals(
        OpenSearchDataType.MappingType.Keyword, nestedFieldType(keywordSide, "attrs", "env"));
    assertEquals(OpenSearchDataType.MappingType.Text, nestedFieldType(textSide, "attrs", "env"));
  }

  private static OpenSearchDataType nestedObject(
      String innerField, OpenSearchDataType.MappingType innerType) {
    return OpenSearchDataType.of(
        OpenSearchDataType.MappingType.Object,
        Map.of("properties", Map.of(innerField, Map.of("type", innerType.toString()))));
  }

  /** Empty object merged first must accept a populated sibling's leaf (regression: #5791). */
  @Test
  void getFieldTypesMergesEmptyObjectWithPopulatedSibling() {
    Map<String, Object> emptyLeaf = Map.of("type", "object");
    Map<String, Object> populatedLeaf =
        Map.of("type", "object", "properties", Map.of("leaf", Map.of("type", "keyword")));
    when(mapping.getFieldMappings()).thenReturn(Map.of("a", nestedObjectTo("c", emptyLeaf)));
    when(mapping2.getFieldMappings()).thenReturn(Map.of("a", nestedObjectTo("c", populatedLeaf)));
    // Empty side first so it becomes the accumulator whose c has no properties.
    when(client.getIndexMappings("idx-*"))
        .thenReturn(ImmutableMap.of("idx-empty", mapping, "idx-populated", mapping2));

    OpenSearchDescribeIndexRequest request = new OpenSearchDescribeIndexRequest(client, "idx-*");
    Map<String, OpenSearchDataType> result = request.getFieldTypes();

    OpenSearchDataType c = result.get("a").getProperties().get("b").getProperties().get("c");
    assertEquals(
        OpenSearchDataType.MappingType.Keyword, c.getProperties().get("leaf").getMappingType());
  }

  /** Builds {@code a: object -> b: object -> c}, with {@code c}'s inner mapping supplied. */
  private static OpenSearchDataType nestedObjectTo(String leafField, Map<String, Object> leaf) {
    return OpenSearchDataType.of(
        OpenSearchDataType.MappingType.Object,
        Map.of(
            "type",
            "object",
            "properties",
            Map.of("b", Map.of("type", "object", "properties", Map.of(leafField, leaf)))));
  }

  private static OpenSearchDataType.MappingType nestedFieldType(
      Map<String, OpenSearchDataType> mappings, String parent, String child) {
    return mappings.get(parent).getProperties().get(child).getMappingType();
  }

  @Test
  void testSearch() {
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    when(client.getIndexMappings("index")).thenReturn(ImmutableMap.of("test", mapping));

    final List<ExprValue> results = new OpenSearchDescribeIndexRequest(client, "index").search();
    assertEquals(1, results.size());
    assertThat(
        results.get(0).tupleValue(),
        anyOf(
            hasEntry("TABLE_NAME", stringValue("index")),
            hasEntry("COLUMN_NAME", stringValue("name")),
            hasEntry("TYPE_NAME", stringValue("STRING"))));
  }

  @Test
  void testSearchWithLangSpec() {
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    when(client.getIndexMappings("index")).thenReturn(ImmutableMap.of("test", mapping));

    final List<ExprValue> results =
        new OpenSearchDescribeIndexRequest(client, "index", PPL_SPEC).search();
    assertEquals(1, results.size());
    assertThat(
        results.get(0).tupleValue(),
        anyOf(
            hasEntry("TABLE_NAME", stringValue("index")),
            hasEntry("COLUMN_NAME", stringValue("name")),
            hasEntry("TYPE_NAME", stringValue("STRING"))));
  }

  /**
   * Only PPL with Calcite enabled names a knn_vector field vector; PPL with Calcite off says array
   * and SQL keeps the mapping type name, as before.
   */
  @Test
  void testKnnVectorTypeNameIsVectorInPplAndKnnVectorInSql() {
    when(mapping.getFieldMappings())
        .thenReturn(
            OpenSearchDataType.parseMapping(
                Map.of("embedding", Map.of("type", "knn_vector", "dimension", 3))));
    when(client.getIndexMappings("index")).thenReturn(ImmutableMap.of("test", mapping));

    assertEquals(
        stringValue("vector"),
        new OpenSearchDescribeIndexRequest(client, "index", PPL_SPEC, true)
            .search()
            .getFirst()
            .tupleValue()
            .get("TYPE_NAME"));
    assertEquals(
        stringValue("array"),
        new OpenSearchDescribeIndexRequest(client, "index", PPL_SPEC)
            .search()
            .getFirst()
            .tupleValue()
            .get("TYPE_NAME"));
    assertEquals(
        stringValue("knn_vector"),
        new OpenSearchDescribeIndexRequest(client, "index")
            .search()
            .getFirst()
            .tupleValue()
            .get("TYPE_NAME"));
  }

  /** The client resolves "cluster:index" itself, so the name reaches it unchanged. */
  @Test
  void testCrossClusterNameReachesTheClientUnchanged() {
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    when(client.getIndexMappings("ccs:index")).thenReturn(ImmutableMap.of("ccs:index", mapping));

    final List<ExprValue> results =
        new OpenSearchDescribeIndexRequest(client, "ccs:index").search();
    assertEquals(1, results.size());
    assertThat(
        results.get(0).tupleValue(),
        anyOf(
            hasEntry("TABLE_NAME", stringValue("index")),
            hasEntry("COLUMN_NAME", stringValue("name")),
            hasEntry("TYPE_NAME", stringValue("STRING"))));
  }

  private static final Map<String, Map<String, Object>> FIELD_MAPPINGS =
      Map.of(
          "knn768", Map.of("type", "knn_vector", "dimension", 768),
          "knn384", Map.of("type", "knn_vector", "dimension", 384),
          "knnByte", Map.of("type", "knn_vector", "dimension", 768, "data_type", "byte"),
          "nested",
              Map.of("type", "nested", "properties", Map.of("sub", Map.of("type", "integer"))),
          "object",
              Map.of("type", "object", "properties", Map.of("sub", Map.of("type", "integer"))));

  /**
   * SQL DESCRIBE of field {@code f} across indices that map it as the named mappings, in order, as
   * {@code column:type} pairs.
   */
  private static String describeField(String... mappings) {
    Map<String, IndexMapping> indices = new LinkedHashMap<>();
    for (int i = 0; i < mappings.length; i++) {
      indices.put(
          "i" + i,
          new IndexMapping(
              OpenSearchDataType.parseMapping(Map.of("f", FIELD_MAPPINGS.get(mappings[i])))));
    }
    OpenSearchClient client = mock(OpenSearchClient.class);
    when(client.getIndexMappings("i*")).thenReturn(indices);
    return new OpenSearchDescribeIndexRequest(client, "i*")
        .search().stream()
            .map(
                row ->
                    row.tupleValue().get("COLUMN_NAME").stringValue()
                        + ":"
                        + row.tupleValue().get("TYPE_NAME").stringValue())
            .collect(Collectors.joining(" "));
  }

  /**
   * Three indices that map a field as knn_vector, nested or object, in every order: the field and
   * its sub-fields are described as when knn_vector merged as ARRAY. A knn_vector and a nested
   * field deep-merge, keeping the first index's type and every sub-field; an object conflicts with
   * either, and the last index's type wins.
   */
  @Test
  void multiIndexVectorFieldsMergeAsBefore() {
    String vectorWithSub = "f:knn_vector f.sub:integer";
    String nestedWithSub = "f:nested f.sub:integer";
    String objectWithSub = "f:object f.sub:integer";
    String vector = "f:knn_vector";
    for (String other : List.of("knn384", "knnByte")) {
      assertAll(
          other,
          () -> assertEquals(vectorWithSub, describeField("knn768", other, "nested")),
          () -> assertEquals(vectorWithSub, describeField("knn768", "nested", other)),
          () -> assertEquals(vectorWithSub, describeField(other, "knn768", "nested")),
          () -> assertEquals(vectorWithSub, describeField(other, "nested", "knn768")),
          () -> assertEquals(nestedWithSub, describeField("nested", "knn768", other)),
          () -> assertEquals(nestedWithSub, describeField("nested", other, "knn768")),
          () -> assertEquals(objectWithSub, describeField("knn768", other, "object")),
          () -> assertEquals(vector, describeField("knn768", "object", other)),
          () -> assertEquals(objectWithSub, describeField(other, "knn768", "object")),
          () -> assertEquals(vector, describeField(other, "object", "knn768")),
          () -> assertEquals(vector, describeField("object", "knn768", other)),
          () -> assertEquals(vector, describeField("object", other, "knn768")));
    }
    assertAll(
        () -> assertEquals(objectWithSub, describeField("knn768", "nested", "object")),
        () -> assertEquals(nestedWithSub, describeField("knn768", "object", "nested")),
        () -> assertEquals(objectWithSub, describeField("nested", "knn768", "object")),
        () -> assertEquals(vector, describeField("nested", "object", "knn768")),
        () -> assertEquals(vectorWithSub, describeField("object", "knn768", "nested")),
        () -> assertEquals(nestedWithSub, describeField("object", "nested", "knn768")));
  }

  /** The merged knn_vector type of a multi-index query names the indices whose mappings differ. */
  @Test
  void multiIndexVectorFieldNamesTheIndicesOfAMismatch() {
    Map<String, IndexMapping> indices = new LinkedHashMap<>();
    indices.put(
        "b",
        new IndexMapping(
            OpenSearchDataType.parseMapping(Map.of("f", FIELD_MAPPINGS.get("knn768")))));
    indices.put(
        "a",
        new IndexMapping(
            OpenSearchDataType.parseMapping(Map.of("f", FIELD_MAPPINGS.get("knn384")))));
    OpenSearchClient client = mock(OpenSearchClient.class);
    when(client.getIndexMappings("a", "b")).thenReturn(indices);
    OpenSearchKnnVectorType merged =
        (OpenSearchKnnVectorType)
            new OpenSearchDescribeIndexRequest(client, "a,b").getFieldTypes().get("f");
    assertTrue(merged.hasMappingMismatch());
    assertEquals(List.of("a", "b"), merged.getMappingMismatchIndices());
    // The fetched mappings are not annotated.
    assertFalse(
        ((OpenSearchKnnVectorType) indices.get("a").getFieldMappings().get("f"))
            .hasMappingMismatch());
  }

  @Test
  void testToString() {
    assertEquals(
        "OpenSearchDescribeIndexRequest{indexName='index'}",
        new OpenSearchDescribeIndexRequest(client, "index").toString());
  }

  @Test
  void testMergeObjectInsideMap() {
    OpenSearchDescribeIndexRequest openSearchDescribeIndexRequest =
        new OpenSearchDescribeIndexRequest(client, "index");
    when(mapping.getFieldMappings()).thenReturn(prepareMap1());
    when(mapping2.getFieldMappings()).thenReturn(prepareMap2());
    when(client.getIndexMappings("index"))
        .thenReturn(ImmutableMap.of("test1", mapping, "test2", mapping2));
    Map<String, OpenSearchDataType> result = openSearchDescribeIndexRequest.getFieldTypes();
    assertEquals(1, result.size());
    assertEquals(result.get("name"), prepareMapResult().get("name"));
  }

  @Test
  void testMergeObjectInsideMapWithLatest() {
    OpenSearchDescribeIndexRequest openSearchDescribeIndexRequest =
        new OpenSearchDescribeIndexRequest(client, "index");
    when(mapping.getFieldMappings())
        .thenReturn(
            Map.of(
                "name",
                OpenSearchDataType.of(
                    OpenSearchDataType.MappingType.Object,
                    Map.of(
                        "type", "object", "properties", Map.of("attr", Map.of("type", "text"))))));
    when(mapping2.getFieldMappings())
        .thenReturn(
            Map.of(
                "name",
                OpenSearchDataType.of(
                    OpenSearchDataType.MappingType.Object,
                    Map.of(
                        "type",
                        "object",
                        "properties",
                        Map.of("attr", Map.of("type", "integer"))))));
    when(client.getIndexMappings("index"))
        .thenReturn(ImmutableMap.of("test1", mapping, "test2", mapping2));
    Map<String, OpenSearchDataType> result = openSearchDescribeIndexRequest.getFieldTypes();
    assertEquals(1, result.size());
    assertEquals(
        result.get("name"),
        OpenSearchDataType.of(
            OpenSearchDataType.MappingType.Object,
            Map.of("type", "object", "properties", Map.of("attr", Map.of("type", "integer")))));
  }

  private Map<String, OpenSearchDataType> prepareMap1() {
    Map<String, OpenSearchDataType> map = new HashMap<>();
    Map<String, Object> innerMap = new LinkedHashMap<>();
    innerMap.put("type", "object");
    innerMap.put(
        "properties",
        Map.of(
            "attr1",
            Map.of("type", "text"),
            "recursive",
            Map.of("type", "nested", "properties", Map.of("attr1", Map.of("type", "text")))));
    map.put("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object, innerMap));
    return map;
  }

  private Map<String, OpenSearchDataType> prepareMap2() {
    Map<String, OpenSearchDataType> map = new HashMap<>();
    Map<String, Object> innerMap = new LinkedHashMap<>();
    innerMap.put("type", "object");
    innerMap.put(
        "properties",
        Map.of(
            "attr2",
            Map.of("type", "text"),
            "recursive",
            Map.of("type", "nested", "properties", Map.of("attr2", Map.of("type", "text")))));
    map.put("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object, innerMap));
    return map;
  }

  private Map<String, OpenSearchDataType> prepareMapResult() {
    Map<String, OpenSearchDataType> map = new HashMap<>();
    Map<String, Object> innerMap = new LinkedHashMap<>();
    innerMap.put("type", "object");
    innerMap.put(
        "properties",
        Map.of(
            "attr2",
            Map.of("type", "text"),
            "attr1",
            Map.of("type", "text"),
            "recursive",
            Map.of(
                "type",
                "nested",
                "properties",
                Map.of("attr1", Map.of("type", "text"), "attr2", Map.of("type", "text")))));
    map.put("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object, innerMap));
    return map;
  }
}
