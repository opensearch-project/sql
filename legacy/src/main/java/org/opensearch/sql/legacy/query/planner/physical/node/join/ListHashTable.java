/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.legacy.query.planner.physical.Row;

/** List implementation to avoid normal hash table degrading into linked list. */
public class ListHashTable<T> implements HashTable<T> {

  private final List<Row<T>> rows = new ArrayList<>();

  @Override
  public void add(Row<T> row) {
    rows.add(row);
  }

  @Override
  public Collection<Row<T>> match(Row<T> row) {
    return rows;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Collection<Object>>[] rightFieldWithLeftValues() {
    return new Map[] {new HashMap()};
  }

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public boolean isEmpty() {
    return rows.isEmpty();
  }

  @Override
  public void clear() {
    rows.clear();
  }
}
