/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;

import com.google.common.collect.Ordering;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.utils.ExprValueOrdering;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.TakeOrderedOperator.ExprOrdering.ExprOrderingBuilder;

/**
 * TakeOrdered Operator. This operator will sort input data as the order of {@link this#sortList}
 * specifies and return {@link this#limit} rows from the {@link this#offset} index.
 *
 * <p>Functionally, this operator is a combination of {@link SortOperator} and {@link
 * LimitOperator}. But it can reduce the time complexity from O(nlogn) to O(n), and memory from O(n)
 * to O(k) due to use guava {@link com.google.common.collect.Ordering}.
 *
 * <p>Overall, it's an optimization to replace `Limit(Sort)` in physical plan level since it's all
 * about execution. Because most execution engine may not support this logical operator, it doesn't
 * have a related logical operator.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class TakeOrderedOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;

  @Getter private final List<Pair<SortOption, Expression>> sortList;
  @Getter private final Integer limit;
  @Getter private final Integer offset;
  @EqualsAndHashCode.Exclude private final ExprOrdering ordering;
  @EqualsAndHashCode.Exclude private Iterator<ExprValue> iterator;

  /**
   * Sort Operator Constructor.
   *
   * @param input input {@link PhysicalPlan}
   * @param limit the limit value from LimitOperator
   * @param offset the offset value from LimitOperator
   * @param sortList list of sort field from SortOperator
   */
  public TakeOrderedOperator(
      PhysicalPlan input,
      Integer limit,
      Integer offset,
      List<Pair<SortOption, Expression>> sortList) {
    this.input = input;
    this.sortList = sortList;
    this.limit = limit;
    this.offset = offset;
    ExprOrderingBuilder orderingBuilder = ExprOrdering.builder();
    for (Pair<SortOption, Expression> pair : sortList) {
      SortOption option = pair.getLeft();
      ExprValueOrdering ordering =
          ASC.equals(option.getSortOrder())
              ? ExprValueOrdering.natural()
              : ExprValueOrdering.natural().reverse();
      ordering =
          NULL_FIRST.equals(option.getNullOrder()) ? ordering.nullsFirst() : ordering.nullsLast();
      orderingBuilder.comparator(Pair.of(pair.getRight(), ordering));
    }
    this.ordering = orderingBuilder.build();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  @Override
  public void open() {
    super.open();
    iterator = ordering.leastOf(input, offset + limit).stream().skip(offset).iterator();
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

  @Builder
  public static class ExprOrdering extends Ordering<ExprValue> {
    @Singular private final List<Pair<Expression, Comparator<ExprValue>>> comparators;

    @Override
    public int compare(ExprValue o1, ExprValue o2) {
      for (Pair<Expression, Comparator<ExprValue>> comparator : comparators) {
        Expression expression = comparator.getKey();
        int result =
            comparator
                .getValue()
                .compare(
                    expression.valueOf(o1.bindingTuples()), expression.valueOf(o2.bindingTuples()));
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }
  }
}
