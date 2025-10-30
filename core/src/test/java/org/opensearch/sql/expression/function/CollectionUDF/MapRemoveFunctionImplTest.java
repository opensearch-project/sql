/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class MapRemoveFunctionImplTest {

  @Test
  public void testMapRemoveWithNullMap() {
    Object result = MapRemoveFunctionImpl.mapRemove(null, Arrays.asList("key1", "key2"));
    assertNull(result);
  }

  @Test
  public void testMapRemoveWithNullKeys() {
    Map<String, Object> map = getBaseMap();

    Object result = MapRemoveFunctionImpl.mapRemove(map, null);
    assertEquals(map, result);
  }

  @Test
  public void testMapRemoveWithInvalidMapArgument() {
    String notAMap = "not a map";
    List<String> keysToRemove = Arrays.asList("key1");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> MapRemoveFunctionImpl.mapRemove(notAMap, keysToRemove));

    assertEquals(
        "First argument must be a map, got: class java.lang.String", exception.getMessage());
  }

  @Test
  public void testMapRemoveWithInvalidKeysArgument() {
    Map<String, Object> map = getBaseMap();
    String notAList = "not a list";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> MapRemoveFunctionImpl.mapRemove(map, notAList));

    assertEquals(
        "Second argument must be an array/list, got: class java.lang.String",
        exception.getMessage());
  }

  @Test
  public void testMapRemoveExistingKeys() {
    Map<String, Object> map = getBaseMap();
    List<String> keysToRemove = Arrays.asList("key1", "key3");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(1, result.size());
    assertEquals("value2", result.get("key2"));
    assertNull(result.get("key1"));
    assertNull(result.get("key3"));

    // Verify original map is not modified
    assertEqualToBaseMap(map);
  }

  @Test
  public void testMapRemoveNonExistingKeys() {
    Map<String, Object> map = getBaseMap();
    List<String> keysToRemove = Arrays.asList("key4", "key5");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEqualToBaseMap(result);
  }

  @Test
  public void testMapRemoveEmptyKeysList() {
    Map<String, Object> map = getBaseMap();
    List<String> keysToRemove = Arrays.asList();

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEqualToBaseMap(result);
  }

  @Test
  public void testMapRemoveMixedExistingAndNonExistingKeys() {
    Map<String, Object> map = getBaseMap();
    List<String> keysToRemove = Arrays.asList("key1", "key4", "key2");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(1, result.size());
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testMapRemoveWithNullKeysInList() {
    Map<String, Object> map = getBaseMap();
    List<Object> keysToRemove = Arrays.asList("key1", null, "key3");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(1, result.size());
    assertEquals("value2", result.get("key2"));
  }

  @Test
  public void testMapRemoveWithDifferentValueTypes() {
    Map<String, Object> map = new HashMap<>();
    map.put("string", "value");
    map.put("number", 42);
    map.put("boolean", true);
    map.put("list", Arrays.asList(1, 2, 3));
    List<String> keysToRemove = Arrays.asList("number", "boolean");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(2, result.size());
    assertEquals("value", result.get("string"));
    assertEquals(Arrays.asList(1, 2, 3), result.get("list"));
    assertNull(result.get("number"));
    assertNull(result.get("boolean"));
  }

  @Test
  public void testMapRemoveAllKeys() {
    Map<String, Object> map = getBaseMap();
    List<String> keysToRemove = Arrays.asList("key1", "key2", "key3");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(0, result.size());
  }

  @Test
  public void testMapRemoveWithEmptyMap() {
    Map<String, Object> map = new HashMap<>();
    List<String> keysToRemove = Arrays.asList("key1", "key2");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(0, result.size());
  }

  @Test
  public void testMapRemoveWithNonStringKeys() {
    Map<String, Object> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    map.put("123", "numeric_key_value");

    List<Object> keysToRemove = Arrays.asList("key1", 123); // 123 will be converted to string "123"

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) MapRemoveFunctionImpl.mapRemove(map, keysToRemove);

    assertEquals(1, result.size());
    assertEquals("value2", result.get("key2"));
  }

  private Map<String, Object> getBaseMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    map.put("key3", "value3");
    return map;
  }

  private void assertEqualToBaseMap(Map<String, Object> map) {
    assertEquals(3, map.size());
    assertEquals("value1", map.get("key1"));
    assertEquals("value2", map.get("key2"));
    assertEquals("value3", map.get("key3"));
  }
}
