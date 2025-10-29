/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.utils.ComparableLinkedHashMap;

public class ComparableLinkedHashMapTest {

  @Test
  public void testEmptyMaps() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();

    assertEquals(0, map1.compareTo(map2));
  }

  @Test
  public void testOneEmptyMap() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);

    assertTrue(map1.compareTo(map2) < 0);
    assertTrue(map2.compareTo(map1) > 0);
  }

  @Test
  public void testDifferentKeys() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("b", 1);

    assertTrue(map1.compareTo(map2) < 0);
  }

  @Test
  public void testEqualMaps() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);
    map2.put("b", 2);

    assertEquals(0, map1.compareTo(map2));
  }

  @Test
  public void testDifferentFirstValue() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 3);
    map2.put("b", 2);

    assertTrue(map1.compareTo(map2) < 0);
    assertTrue(map2.compareTo(map1) > 0);
  }

  @Test
  public void testDifferentLaterValue() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);
    map1.put("c", 3);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);
    map2.put("b", 3);
    map2.put("c", 3);

    assertTrue(map1.compareTo(map2) < 0);
    assertTrue(map2.compareTo(map1) > 0);
  }

  @Test
  public void testDifferentSizes() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);

    assertTrue(map1.compareTo(map2) > 0);
    assertTrue(map2.compareTo(map1) < 0);
  }

  @Test
  public void testNullValues() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", null);
    map1.put("b", 2);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);
    map2.put("b", 2);

    assertTrue(map1.compareTo(map2) < 0);
    assertTrue(map2.compareTo(map1) > 0);

    ComparableLinkedHashMap<String, Integer> map3 = new ComparableLinkedHashMap<>();
    map3.put("a", null);
    map3.put("b", 2);

    assertEquals(0, map1.compareTo(map3));
  }

  @Test
  public void testCustomObjects() {
    class Person {
      String name;

      Person(String name) {
        this.name = name;
      }

      @Override
      public String toString() {
        return name;
      }
    }

    ComparableLinkedHashMap<String, Person> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", new Person("Alice"));
    map1.put("b", new Person("Bob"));

    ComparableLinkedHashMap<String, Person> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", new Person("Alice"));
    map2.put("b", new Person("Charlie"));

    assertTrue(map1.compareTo(map2) < 0);
    assertTrue(map2.compareTo(map1) > 0);
  }

  @Test
  public void testMixedTypes() {
    ComparableLinkedHashMap<String, Object> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", "test");

    ComparableLinkedHashMap<String, Object> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);
    map2.put("b", "test");

    assertEquals(0, map1.compareTo(map2));

    ComparableLinkedHashMap<String, Object> map3 = new ComparableLinkedHashMap<>();
    map3.put("a", 1);
    map3.put("b", "test2");

    assertTrue(map1.compareTo(map3) < 0);
    assertTrue(map3.compareTo(map1) > 0);
  }

  @Test
  public void testWithTreeSet() {
    TreeSet<ComparableLinkedHashMap<String, Integer>> set = new TreeSet<>();

    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 1);
    map2.put("b", 3);

    ComparableLinkedHashMap<String, Integer> map3 = new ComparableLinkedHashMap<>();
    map3.put("a", 0);
    map3.put("b", 4);

    set.add(map2);
    set.add(map1);
    set.add(map3);

    Iterator<ComparableLinkedHashMap<String, Integer>> iterator = set.iterator();
    ComparableLinkedHashMap<String, Integer> first = iterator.next();
    ComparableLinkedHashMap<String, Integer> second = iterator.next();
    ComparableLinkedHashMap<String, Integer> third = iterator.next();

    assertEquals(Integer.valueOf(0), first.get("a"));
    assertEquals(Integer.valueOf(4), first.get("b"));

    assertEquals(Integer.valueOf(1), second.get("a"));
    assertEquals(Integer.valueOf(2), second.get("b"));

    assertEquals(Integer.valueOf(1), third.get("a"));
    assertEquals(Integer.valueOf(3), third.get("b"));

    assertEquals(3, set.size());
  }

  @Test
  public void testWithComparator() {
    Comparator<ComparableLinkedHashMap<String, Integer>> comparator =
        ComparableLinkedHashMap::compareTo;

    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 5);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 3);

    assertTrue(comparator.compare(map1, map2) > 0);
    assertTrue(comparator.compare(map2, map1) < 0);
  }

  @Test
  public void testEqualValuesDifferentKeys() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);
    map1.put("c", 3);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("d", 1);
    map2.put("e", 2);
    map2.put("f", 3);

    assertTrue(map1.compareTo(map2) < 0);
  }

  @Test
  public void testDifferentOrder() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", 2);
    map2.put("b", 1);

    assertTrue(map1.compareTo(map2) < 0);
    assertTrue(map2.compareTo(map1) > 0);
  }

  @Test
  public void testLargeMaps() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      map1.put("key" + i, i);
    }

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      map2.put("key" + i, i);
    }

    assertEquals(0, map1.compareTo(map2));

    map2.put("key", 100);
    assertTrue(map1.compareTo(map2) < 0);
  }
}
