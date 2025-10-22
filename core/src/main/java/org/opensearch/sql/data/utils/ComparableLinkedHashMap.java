/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import java.util.LinkedHashMap;

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
    if (this.isEmpty() && other.isEmpty()) {
      return 0;
    }
    if (this.isEmpty()) {
      return -1;
    }
    if (other.isEmpty()) {
      return 1;
    }

    V thisFirstValue = this.values().iterator().next();
    V otherFirstValue = other.values().iterator().next();

    if (thisFirstValue instanceof Comparable) {
      return ((Comparable) thisFirstValue).compareTo(otherFirstValue);
    }

    return thisFirstValue.toString().compareTo(otherFirstValue.toString());
  }
}
