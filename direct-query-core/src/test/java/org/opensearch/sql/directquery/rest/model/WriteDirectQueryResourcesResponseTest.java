/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class WriteDirectQueryResourcesResponseTest {

  @Test
  public void testDefaultConstructor() {
    WriteDirectQueryResourcesResponse<String> response = new WriteDirectQueryResourcesResponse<>();
    assertNull(response.getData());
  }

  @Test
  public void testWithStringList() {
    List<String> stringList = Arrays.asList("rule1", "rule2", "rule3");
    WriteDirectQueryResourcesResponse<List<String>> response =
        WriteDirectQueryResourcesResponse.withStringList(stringList);

    assertNotNull(response);
    assertEquals(stringList, response.getData());
  }

  @Test
  public void testWithList() {
    List<Integer> integerList = Arrays.asList(1, 2, 3);
    WriteDirectQueryResourcesResponse<List<Integer>> response =
        WriteDirectQueryResourcesResponse.withList(integerList);

    assertNotNull(response);
    assertEquals(integerList, response.getData());
  }

  @Test
  public void testWithMap() {
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("rule1", "active");
    stringMap.put("rule2", "inactive");

    WriteDirectQueryResourcesResponse<Map<String, String>> response =
        WriteDirectQueryResourcesResponse.withMap(stringMap);

    assertNotNull(response);
    assertEquals(stringMap, response.getData());
  }

  @Test
  public void testWithMapGeneric() {
    Map<String, Boolean> booleanMap = new HashMap<>();
    booleanMap.put("rule1", true);
    booleanMap.put("rule2", false);

    WriteDirectQueryResourcesResponse<Map<String, Boolean>> response =
        WriteDirectQueryResourcesResponse.withMap(booleanMap);

    assertNotNull(response);
    assertEquals(booleanMap, response.getData());
  }

  @Test
  public void testWithNullData() {
    WriteDirectQueryResourcesResponse<List<String>> response =
        WriteDirectQueryResourcesResponse.withStringList(null);

    assertNotNull(response);
    assertNull(response.getData());
  }

  @Test
  public void testSetterAndGetter() {
    WriteDirectQueryResourcesResponse<String> response = new WriteDirectQueryResourcesResponse<>();
    String testData = "test-response-data";

    response.setData(testData);
    assertEquals(testData, response.getData());
  }

  @Test
  public void testLombokDataAnnotation() {
    List<String> data = Arrays.asList("item1", "item2");

    WriteDirectQueryResourcesResponse<List<String>> response1 =
        WriteDirectQueryResourcesResponse.withList(data);
    WriteDirectQueryResourcesResponse<List<String>> response2 =
        WriteDirectQueryResourcesResponse.withList(data);

    assertEquals(response1, response2);
    assertEquals(response1.hashCode(), response2.hashCode());
    assertEquals(response1.toString(), response2.toString());
  }

  @Test
  public void testEqualsAndHashCodeDifferentData() {
    List<String> data1 = Arrays.asList("item1");
    List<String> data2 = Arrays.asList("item2");

    WriteDirectQueryResourcesResponse<List<String>> response1 =
        WriteDirectQueryResourcesResponse.withList(data1);
    WriteDirectQueryResourcesResponse<List<String>> response2 =
        WriteDirectQueryResourcesResponse.withList(data2);

    assertNotEquals(response1, response2);
    assertNotEquals(response1.hashCode(), response2.hashCode());
  }

  @Test
  public void testJsonSerializationWithNonNullAnnotation() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();

    // Test with null data - should not include data field
    WriteDirectQueryResourcesResponse<String> responseWithNull = new WriteDirectQueryResourcesResponse<>();
    String jsonWithNull = objectMapper.writeValueAsString(responseWithNull);
    assertEquals("{}", jsonWithNull);

    // Test with data - should include data field
    List<String> data = Arrays.asList("test1", "test2");
    WriteDirectQueryResourcesResponse<List<String>> responseWithData =
        WriteDirectQueryResourcesResponse.withStringList(data);
    String jsonWithData = objectMapper.writeValueAsString(responseWithData);
    assertTrue(jsonWithData.contains("\"data\""));
    assertTrue(jsonWithData.contains("test1"));
    assertTrue(jsonWithData.contains("test2"));
  }

  @Test
  public void testJsonDeserialization() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    String json = "{\"data\":[\"rule1\",\"rule2\"]}";

    WriteDirectQueryResourcesResponse response =
        objectMapper.readValue(json, WriteDirectQueryResourcesResponse.class);

    assertNotNull(response.getData());
    assertTrue(response.getData() instanceof List);
  }

  @Test
  public void testToStringContainsData() {
    List<String> data = Arrays.asList("metric1", "metric2");
    WriteDirectQueryResourcesResponse<List<String>> response =
        WriteDirectQueryResourcesResponse.withStringList(data);

    String toString = response.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("metric1") || toString.contains("data"));
  }

  @Test
  public void testWithEmptyList() {
    List<String> emptyList = Arrays.asList();
    WriteDirectQueryResourcesResponse<List<String>> response =
        WriteDirectQueryResourcesResponse.withStringList(emptyList);

    assertNotNull(response.getData());
    assertTrue(response.getData().isEmpty());
  }

  @Test
  public void testWithEmptyMap() {
    Map<String, String> emptyMap = new HashMap<>();
    WriteDirectQueryResourcesResponse<Map<String, String>> response =
        WriteDirectQueryResourcesResponse.withMap(emptyMap);

    assertNotNull(response.getData());
    assertTrue(response.getData().isEmpty());
  }

  @Test
  public void testGenericTypePreservation() {
    Map<String, Integer> integerMap = new HashMap<>();
    integerMap.put("count", 42);

    WriteDirectQueryResourcesResponse<Map<String, Integer>> response =
        WriteDirectQueryResourcesResponse.withMap(integerMap);

    assertNotNull(response.getData());
    assertEquals(Integer.valueOf(42), response.getData().get("count"));
  }
}