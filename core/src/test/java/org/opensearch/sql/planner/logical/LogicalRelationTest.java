/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LogicalRelationTest {

  @Test
  public void logicalRelationHasNoInput() {
    LogicalPlan relation = LogicalPlanDSL.relation("index");
    assertEquals(0, relation.getChild().size());
  }
}
