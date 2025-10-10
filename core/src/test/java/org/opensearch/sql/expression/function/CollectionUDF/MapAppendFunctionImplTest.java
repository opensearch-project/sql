/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapAppendFunctionImplTest {
  @Test
  void testMapAppendWithNonOverlappingKeys() {
    Map<String, Object> map1 = getMap1();
    Map<String, Object> map2 = getMap2();

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertEquals(4, result.size());
    assertMapListValues(result, "a", "value1");
    assertMapListValues(result, "b", "value2");
    assertMapListValues(result, "c", "value3");
    assertMapListValues(result, "d", "value4");
  }

  @Test
  void testMapAppendWithOverlappingKeys() {
    Map<String, Object> map1 = getMap1();
    Map<String, Object> map2 = Map.of("b", "value3", "c", "value4");

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertEquals(3, result.size());
    assertMapListValues(result, "a", "value1");
    assertMapListValues(result, "b", "value2", "value3");
    assertMapListValues(result, "c", "value4");
  }

  @Test
  void testMapAppendWithArrayValues() {
    Map<String, Object> map1 = Map.of("a", List.of("item1", "item2"), "b", "single");
    Map<String, Object> map2 = Map.of("a", "item3", "c", List.of("item4", "item5"));

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertEquals(3, result.size());
    assertMapListValues(result, "a", "item1", "item2", "item3");
    assertMapListValues(result, "b", "single");
    assertMapListValues(result, "c", "item4", "item5");
  }

  @Test
  void testMapAppendWithNullValues() {
    Map<String, Object> map1 = getMap1();
    map1.put("b", null);
    Map<String, Object> map2 = getMap2();
    map2.put("b", "value2");
    map2.put("a", null);

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertEquals(4, result.size());
    assertMapListValues(result, "a", "value1");
    assertMapListValues(result, "b", "value2");
    assertMapListValues(result, "c", "value3");
    assertMapListValues(result, "d", "value4");
  }

  @Test
  void testMapAppendWithSingleParam() {
    Map<String, Object> map1 = getMap1();

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1);

    assertEquals(2, result.size());
    assertMapListValues(result, "a", "value1");
    assertMapListValues(result, "b", "value2");
  }

  private Map<String, Object> getMap1() {
    Map<String, Object> map1 = new HashMap<>();
    map1.put("a", "value1");
    map1.put("b", "value2");
    return map1;
  }

  private Map<String, Object> getMap2() {
    Map<String, Object> map2 = new HashMap<>();
    map2.put("c", "value3");
    map2.put("d", "value4");
    return map2;
  }

  private void assertMapListValues(Map<String, Object> map, String key, Object... expectedValues) {
    Object val = map.get(key);
    assertTrue(val instanceof List);
    List<Object> result = (List<Object>) val;
    assertEquals(expectedValues.length, result.size());
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], result.get(i));
    }
  }
}
