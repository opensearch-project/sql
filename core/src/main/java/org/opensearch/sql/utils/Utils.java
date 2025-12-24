/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public interface Utils {
  static <I> List<Pair<I, Integer>> zipWithIndex(List<I> input) {
    LinkedList<Pair<I, Integer>> result = new LinkedList<>();
    Iterator<I> iter = input.iterator();
    int index = 0;
    while (iter.hasNext()) {
      result.add(Pair.of(iter.next(), index++));
    }
    return result;
  }
}
