/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.sort;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
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
      return fieldBuild((ReferenceExpression) expression, option);
    } else {
      throw new IllegalStateException("unsupported expression " + expression.getClass());
    }
  }

  private FieldSortBuilder fieldBuild(ReferenceExpression ref, Sort.SortOption option) {
    return SortBuilders.fieldSort(
        OpenSearchTextType.convertTextToKeyword(ref.getAttr(), ref.type()))
        .order(sortOrderMap.get(option.getSortOrder()))
        .missing(missingMap.get(option.getNullOrder()));
  }
}
