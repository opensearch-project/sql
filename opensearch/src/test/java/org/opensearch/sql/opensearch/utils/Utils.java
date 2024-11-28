/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;

@UtilityClass
public class Utils {

  public static AvgAggregator avg(Expression expr, ExprCoreType type) {
    return new AvgAggregator(Collections.singletonList(expr), type);
  }

  public static List<NamedAggregator> agg(NamedAggregator... exprs) {
    return Arrays.asList(exprs);
  }

  public static List<NamedExpression> group(NamedExpression... exprs) {
    return Arrays.asList(exprs);
  }

  public static List<Pair<Sort.SortOption, Expression>> sort(
      Expression expr1, Sort.SortOption option1) {
    return Collections.singletonList(Pair.of(option1, expr1));
  }

  public static List<Pair<Sort.SortOption, Expression>> sort(
      Expression expr1, Sort.SortOption option1, Expression expr2, Sort.SortOption option2) {
    return Arrays.asList(Pair.of(option1, expr1), Pair.of(option2, expr2));
  }
}
