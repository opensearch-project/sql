/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.paginate;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class CreatePagingTableScanBuilderTest {

  @Mock
  LogicalPlan multiRelationPaginate;

  @Mock
  Table table;

  @BeforeEach
  public void setUp() {
    when(multiRelationPaginate.getChild())
        .thenReturn(
            List.of(relation("t1", table), relation("t2", table)));
  }
  @Test
  void throws_when_mutliple_children() {
    final var pattern = new CreatePagingTableScanBuilder().pattern();
    final var plan = paginate(multiRelationPaginate, 42);
    assertThrows(UnsupportedOperationException.class,
        () -> DEFAULT_MATCHER.match(pattern, plan));
  }
}
