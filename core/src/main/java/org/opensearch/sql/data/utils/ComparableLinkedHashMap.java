/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class ComparableLinkedHashMap<K, V> extends LinkedHashMap<K, V>
    implements Comparable<ComparableLinkedHashMap<K, V>> {

  public ComparableLinkedHashMap() {
    super();
  }

  public ComparableLinkedHashMap(int initialCapacity) {
    super(initialCapacity);
  }

  public ComparableLinkedHashMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  @Override
  public int compareTo(ComparableLinkedHashMap<K, V> other) {
    if (this.isEmpty() && other.isEmpty()) return 0;
    if (this.isEmpty()) return -1;
    if (other.isEmpty()) return 1;
    Iterator<Map.Entry<K, V>> thisIterator = this.entrySet().iterator();
    Iterator<Map.Entry<K, V>> otherIterator = other.entrySet().iterator();
    return compareRecursive(thisIterator, otherIterator);
  }

  private int compareRecursive(
      Iterator<Map.Entry<K, V>> thisIterator, Iterator<Map.Entry<K, V>> otherIterator) {
    boolean thisHasNext = thisIterator.hasNext();
    boolean otherHasNext = otherIterator.hasNext();
    if (!thisHasNext && !otherHasNext) return 0;
    if (!thisHasNext) return -1;
    if (!otherHasNext) return 1;

    Map.Entry<K, V> thisEntry = thisIterator.next();
    Map.Entry<K, V> otherEntry = otherIterator.next();
    K thisKey = thisEntry.getKey();
    K otherKey = otherEntry.getKey();
    V thisValue = thisEntry.getValue();
    V otherValue = otherEntry.getValue();
    int comparison = compareKV(thisKey, otherKey, thisValue, otherValue);
    if (comparison != 0) return comparison;
    return compareRecursive(thisIterator, otherIterator);
  }

  @SuppressWarnings("unchecked")
  private int compareKV(K key1, K key2, V value1, V value2) {
    int keyCompare;
    if (key1 instanceof Comparable) {
      keyCompare = ((Comparable<K>) key1).compareTo(key2);
    } else {
      keyCompare = key1.toString().compareTo(key2.toString());
    }
    if (keyCompare != 0) {
      return keyCompare;
    }

    if (value1 == null && value2 == null) return 0;
    if (value1 == null) return -1;
    if (value2 == null) return 1;
    if (value1 instanceof Comparable) {
      return ((Comparable<V>) value1).compareTo(value2);
    }
    return value1.toString().compareTo(value2.toString());
  }
}
