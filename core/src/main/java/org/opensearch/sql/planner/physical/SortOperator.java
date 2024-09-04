/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * Sort Operator.The input data is sorted by the sort fields in the {@link SortOperator#sortList}.
 * The sort field is specified by the {@link Expression} with {@link SortOption}. The count indicate
 * how many sorted result should been return.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class SortOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;

  @Getter private final List<Pair<SortOption, Expression>> sortList;
  @EqualsAndHashCode.Exclude private final Comparator<ExprValue> sorter;
  @EqualsAndHashCode.Exclude private Iterator<ExprValue> iterator;

  /**
   * Sort Operator Constructor.
   *
   * @param input input {@link PhysicalPlan}
   * @param sortList list of sort sort field. The sort field is specified by the {@link Expression}
   *     with {@link SortOption}
   */
  public SortOperator(PhysicalPlan input, List<Pair<SortOption, Expression>> sortList) {
    this.input = input;
    this.sortList = sortList;
    this.sorter = SortHelper.constructExprComparator(sortList);
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  @Override
  public void open() {
    super.open();
    PriorityQueue<ExprValue> sorted = new PriorityQueue<>(1, sorter::compare);
    while (input.hasNext()) {
      sorted.add(input.next());
    }

    iterator = iterator(sorted);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  private Iterator<ExprValue> iterator(PriorityQueue<ExprValue> result) {
    return new Iterator<ExprValue>() {
      @Override
      public boolean hasNext() {
        return !result.isEmpty();
      }

      @Override
      public ExprValue next() {
        return result.poll();
      }
    };
  }
}
