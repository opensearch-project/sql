/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.mapUDF;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class MapMergeFunctionImplTest {

  @Test
  public void testMapMergeWithTwoMaps() {
    Map<String, Object> map1 = new HashMap<>();
    map1.put("name", "John");
    map1.put("age", 30);

    Map<String, Object> map2 = new HashMap<>();
    map2.put("city", "New York");
    map2.put("country", "USA");

    Object result = MapMergeFunctionImpl.eval(map1, map2);

    assertNotNull(result);
    assertTrue(result instanceof Map);

    @SuppressWarnings("unchecked")
    Map<String, Object> resultMap = (Map<String, Object>) result;

    assertEquals(4, resultMap.size());
    assertEquals("John", resultMap.get("name"));
    assertEquals(30, resultMap.get("age"));
    assertEquals("New York", resultMap.get("city"));
    assertEquals("USA", resultMap.get("country"));
  }

  @Test
  public void testMapMergeWithOverlappingKeys() {
    Map<String, Object> map1 = new HashMap<>();
    map1.put("name", "John");
    map1.put("age", 30);

    Map<String, Object> map2 = new HashMap<>();
    map2.put("name", "Jane"); // This should override
    map2.put("city", "New York");

    Object result = MapMergeFunctionImpl.eval(map1, map2);

    assertNotNull(result);
    assertTrue(result instanceof Map);

    @SuppressWarnings("unchecked")
    Map<String, Object> resultMap = (Map<String, Object>) result;

    assertEquals(3, resultMap.size());
    assertEquals("Jane", resultMap.get("name")); // map2 value should win
    assertEquals(30, resultMap.get("age"));
    assertEquals("New York", resultMap.get("city"));
  }

  @Test
  public void testMapMergeWithNullMaps() {
    Object result1 = MapMergeFunctionImpl.eval(null, null);
    assertNull(result1);

    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");

    Object result2 = MapMergeFunctionImpl.eval(null, map);
    assertNotNull(result2);
    assertEquals(map, result2);

    Object result3 = MapMergeFunctionImpl.eval(map, null);
    assertNotNull(result3);
    assertEquals(map, result3);
  }

  @Test
  public void testMapMergeWithWrongArgumentCount() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MapMergeFunctionImpl.eval("single_arg");
        });

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MapMergeFunctionImpl.eval("arg1", "arg2", "arg3");
        });
  }
}
