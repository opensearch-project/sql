/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class GetDirectQueryResourcesResponseTest {

  @Test
  public void testDefaultConstructor() {
    GetDirectQueryResourcesResponse<String> response = new GetDirectQueryResourcesResponse<>();
    assertNull(response.getData());
  }

  @Test
  public void testWithStringList() {
    List<String> stringList = Arrays.asList("label1", "label2", "label3");
    GetDirectQueryResourcesResponse<List<String>> response = 
        GetDirectQueryResourcesResponse.withStringList(stringList);
    
    assertNotNull(response);
    assertEquals(stringList, response.getData());
  }

  @Test
  public void testWithList() {
    List<Integer> integerList = Arrays.asList(1, 2, 3);
    GetDirectQueryResourcesResponse<List<Integer>> response = 
        GetDirectQueryResourcesResponse.withList(integerList);
    
    assertNotNull(response);
    assertEquals(integerList, response.getData());
  }

  @Test
  public void testWithMap() {
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("key1", "value1");
    stringMap.put("key2", "value2");
    
    GetDirectQueryResourcesResponse<Map<String, String>> response = 
        GetDirectQueryResourcesResponse.withMap(stringMap);
    
    assertNotNull(response);
    assertEquals(stringMap, response.getData());
  }

  @Test
  public void testWithMapGeneric() {
    Map<String, Integer> integerMap = new HashMap<>();
    integerMap.put("count1", 10);
    integerMap.put("count2", 20);
    
    GetDirectQueryResourcesResponse<Map<String, Integer>> response = 
        GetDirectQueryResourcesResponse.withMap(integerMap);
    
    assertNotNull(response);
    assertEquals(integerMap, response.getData());
  }

  @Test
  public void testWithNullData() {
    GetDirectQueryResourcesResponse<List<String>> response = 
        GetDirectQueryResourcesResponse.withStringList(null);
    
    assertNotNull(response);
    assertNull(response.getData());
  }

  @Test
  public void testSetterAndGetter() {
    GetDirectQueryResourcesResponse<String> response = new GetDirectQueryResourcesResponse<>();
    String testData = "test-data";
    
    response.setData(testData);
    assertEquals(testData, response.getData());
  }

  @Test
  public void testLombokDataAnnotation() {
    List<String> data = Arrays.asList("item1", "item2");
    
    GetDirectQueryResourcesResponse<List<String>> response1 = 
        GetDirectQueryResourcesResponse.withList(data);
    GetDirectQueryResourcesResponse<List<String>> response2 = 
        GetDirectQueryResourcesResponse.withList(data);
    
    assertEquals(response1, response2);
    assertEquals(response1.hashCode(), response2.hashCode());
    assertEquals(response1.toString(), response2.toString());
  }
}