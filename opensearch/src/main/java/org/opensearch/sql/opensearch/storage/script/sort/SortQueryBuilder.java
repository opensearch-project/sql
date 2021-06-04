/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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
import org.opensearch.sql.opensearch.storage.script.ScriptUtils;

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
    return SortBuilders.fieldSort(ScriptUtils.convertTextToKeyword(ref.getAttr(), ref.type()))
        .order(sortOrderMap.get(option.getSortOrder()))
        .missing(missingMap.get(option.getNullOrder()));
  }
}
