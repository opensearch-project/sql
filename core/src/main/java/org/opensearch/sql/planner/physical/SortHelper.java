package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;

import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.utils.ExprValueOrdering;
import org.opensearch.sql.expression.Expression;

public interface SortHelper {

  static Comparator<ExprValue> constructExprComparator(
      List<Pair<SortOption, Expression>> sortList) {
    return (o1, o2) -> compareWithExpressions(o1, o2, constructComparator(sortList));
  }

  static Ordering<ExprValue> constructExprOrdering(List<Pair<SortOption, Expression>> sortList) {
    return Ordering.from(constructExprComparator(sortList));
  }

  private static List<Pair<Expression, Comparator<ExprValue>>> constructComparator(
      List<Pair<SortOption, Expression>> sortList) {
    List<Pair<Expression, Comparator<ExprValue>>> comparators = new ArrayList<>();
    for (Pair<SortOption, Expression> pair : sortList) {
      SortOption option = pair.getLeft();
      ExprValueOrdering ordering =
          ASC.equals(option.getSortOrder())
              ? ExprValueOrdering.natural()
              : ExprValueOrdering.natural().reverse();
      ordering =
          NULL_FIRST.equals(option.getNullOrder()) ? ordering.nullsFirst() : ordering.nullsLast();
      comparators.add(Pair.of(pair.getRight(), ordering));
    }
    return comparators;
  }

  private static int compareWithExpressions(
      ExprValue o1, ExprValue o2, List<Pair<Expression, Comparator<ExprValue>>> comparators) {
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
