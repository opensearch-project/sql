/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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

    assertThat(
        result,
        allOf(
            hasEntry("a", "value1"),
            hasEntry("b", "value2"),
            hasEntry("c", "value3"),
            hasEntry("d", "value4"),
            aMapWithSize(4)));
  }

  @Test
  void testMapAppendWithOverlappingKeys() {
    Map<String, Object> map1 = getMap1();
    Map<String, Object> map2 = Map.of("b", "value3", "c", "value4");

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertThat(
        result,
        allOf(
            aMapWithSize(3),
            hasEntry("a", (Object) "value1"),
            hasEntry("b", (Object) List.of("value2", "value3")),
            hasEntry("c", (Object) "value4")));
  }

  @Test
  void testMapAppendWithArrayValues() {
    Map<String, Object> map1 = Map.of("a", List.of("item1", "item2"), "b", "single");
    Map<String, Object> map2 = Map.of("a", "item3", "c", List.of("item4", "item5"));

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertThat(
        result,
        allOf(
            aMapWithSize(3),
            hasEntry("a", (Object) List.of("item1", "item2", "item3")),
            hasEntry("b", (Object) "single"),
            hasEntry("c", (Object) List.of("item4", "item5"))));
  }

  @Test
  void testMapAppendWithNullValues() {
    Map<String, Object> map1 = getMap1();
    map1.put("b", null);
    Map<String, Object> map2 = getMap2();
    map2.put("b", "value2");
    map2.put("a", null);

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1, map2);

    assertThat(
        result,
        allOf(
            hasEntry("a", "value1"),
            hasEntry("b", "value2"),
            hasEntry("c", "value3"),
            hasEntry("d", "value4"),
            aMapWithSize(4)));
  }

  @Test
  void testMapAppendWithSingleParam() {
    Map<String, Object> map1 = getMap1();

    Map<String, Object> result = MapAppendFunctionImpl.mapAppendImpl(map1);

    assertThat(result, allOf(hasEntry("a", "value1"), hasEntry("b", "value2"), aMapWithSize(2)));
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
}
