/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

package org.opensearch.sql.opensearch.storage.script.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.opensearch.utils.Utils.sort;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

@ExtendWith(MockitoExtension.class)
class GroupSortOrderTest {

  private final AggregationQueryBuilder.GroupSortOrder groupSortOrder =
      new AggregationQueryBuilder.GroupSortOrder(
          sort(ref("name", STRING), Sort.SortOption.DEFAULT_DESC,
              ref("age", INTEGER), Sort.SortOption.DEFAULT_ASC));
  @Mock
  private ReferenceExpression ref;

  @Test
  void both_expression_in_sort_list() {
    assertEquals(-1, compare(named("name", ref), named("age", ref)));
    assertEquals(1, compare(named("age", ref), named("name", ref)));
    assertEquals(SortOrder.DESC, order(named("name", ref)));
    assertEquals(SortOrder.ASC, order(named("age", ref)));
  }

  @Test
  void only_one_expression_in_sort_list() {
    assertEquals(-1, compare(named("name", ref), named("noSort", ref)));
    assertEquals(1, compare(named("noSort", ref), named("name", ref)));
    assertEquals(SortOrder.DESC, order(named("name", ref)));
    assertEquals(SortOrder.ASC, order(named("noSort", ref)));
  }

  @Test
  void no_expression_in_sort_list() {
    assertEquals(0, compare(named("noSort1", ref), named("noSort2", ref)));
    assertEquals(0, compare(named("noSort2", ref), named("noSort1", ref)));
    assertEquals(SortOrder.ASC, order(named("noSort1", ref)));
    assertEquals(SortOrder.ASC, order(named("noSort2", ref)));
  }

  private int compare(NamedExpression e1, NamedExpression e2) {
    return groupSortOrder.compare(e1, e2);
  }

  private SortOrder order(NamedExpression expr) {
    return groupSortOrder.apply(expr);
  }
}
