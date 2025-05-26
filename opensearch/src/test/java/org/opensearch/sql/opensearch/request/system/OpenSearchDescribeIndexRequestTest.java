/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;

@ExtendWith(MockitoExtension.class)
class OpenSearchDescribeIndexRequestTest {

  @Mock private OpenSearchClient client;

  @Mock private IndexMapping mapping;

  @Mock private IndexMapping mapping2;

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

  @Test
  void testCrossClusterShouldSearchLocal() {
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    when(client.getIndexMappings("index")).thenReturn(ImmutableMap.of("test", mapping));

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
