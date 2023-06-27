/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.sort;

import static org.opensearch.sql.analysis.NestedAnalyzer.generatePath;
import static org.opensearch.sql.analysis.NestedAnalyzer.isNestedFunction;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

/**
 * Builder of {@link SortBuilder}.
 */
public class SortQueryBuilder {

  /**
   * The mapping between Core Engine sort order and OpenSearch sort order.
   */
  private Map<Sort.SortOrder, SortOrder> sortOrderMap =
      new ImmutableMap.Builder<Sort.SortOrder, SortOrder>()
          .put(Sort.SortOrder.ASC, SortOrder.ASC)
          .put(Sort.SortOrder.DESC, SortOrder.DESC)
          .build();

  /**
   * The mapping between Core Engine null order and OpenSearch null order.
   */
  private Map<Sort.NullOrder, String> missingMap =
      new ImmutableMap.Builder<Sort.NullOrder, String>()
          .put(Sort.NullOrder.NULL_FIRST, "_first")
          .put(Sort.NullOrder.NULL_LAST, "_last")
          .build();

  /**
   * Build {@link SortBuilder}.
   *
   * @param expression expression
   * @param option sort option
   * @return SortBuilder.
   */
  public SortBuilder<?> build(Expression expression, Sort.SortOption option) {
    if (expression instanceof ReferenceExpression) {
      if (((ReferenceExpression) expression).getAttr().equalsIgnoreCase("_score")) {
        return SortBuilders.scoreSort().order(sortOrderMap.get(option.getSortOrder()));
      }
      return fieldBuild((ReferenceExpression) expression, option);
    } else if (isNestedFunction(expression)) {

      validateNestedArgs((FunctionExpression) expression);
      String orderByName = ((FunctionExpression)expression).getArguments().get(0).toString();
      // Generate path if argument not supplied in function.
      ReferenceExpression path = ((FunctionExpression)expression).getArguments().size() == 2
          ? (ReferenceExpression) ((FunctionExpression)expression).getArguments().get(1)
          : generatePath(orderByName);
      return SortBuilders.fieldSort(orderByName)
              .order(sortOrderMap.get(option.getSortOrder()))
              .setNestedSort(new NestedSortBuilder(path.toString()));
    } else {
      throw new IllegalStateException("unsupported expression " + expression.getClass());
    }
  }

  /**
   * Validate semantics for arguments in nested function.
   * @param nestedFunc Nested function expression.
   */
  private void validateNestedArgs(FunctionExpression nestedFunc) {
    if (nestedFunc.getArguments().size() > 2) {
      throw new IllegalArgumentException(
          "nested function supports 2 parameters (field, path) or 1 parameter (field)"
      );
    }

    for (Expression arg : nestedFunc.getArguments()) {
      if (!(arg instanceof ReferenceExpression)) {
        throw new IllegalArgumentException(
            String.format("Illegal nested field name: %s",
                arg.toString()
            )
        );
      }
    }
  }

  private FieldSortBuilder fieldBuild(ReferenceExpression ref, Sort.SortOption option) {
    return SortBuilders.fieldSort(
        OpenSearchTextType.convertTextToKeyword(ref.getAttr(), ref.type()))
        .order(sortOrderMap.get(option.getSortOrder()))
        .missing(missingMap.get(option.getNullOrder()));
  }
}
