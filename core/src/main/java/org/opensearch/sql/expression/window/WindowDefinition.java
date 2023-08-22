/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window;

import static org.opensearch.sql.ast.tree.Sort.SortOption;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.Expression;

/** Window definition that consists of partition and sort by information for a window. */
@Data
public class WindowDefinition {

  private final List<Expression> partitionByList;
  private final List<Pair<SortOption, Expression>> sortList;

  /**
   * Return all items in partition by and sort list.
   *
   * @return all sort items
   */
  public List<Pair<SortOption, Expression>> getAllSortItems() {
    List<Pair<SortOption, Expression>> allSorts = new ArrayList<>();
    partitionByList.forEach(expr -> allSorts.add(ImmutablePair.of(DEFAULT_ASC, expr)));
    allSorts.addAll(sortList);
    return allSorts;
  }
}
