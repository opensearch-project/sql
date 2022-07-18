/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.constants.TestConstants;

class LogicalRelationTest {

  @Test
  public void logicalRelationHasNoInput() {
    LogicalPlan relation = LogicalPlanDSL.relation("index");
    assertEquals(0, relation.getChild().size());
  }

  @Test
  public void logicalRelationWithCatalogHasNoInput() {
    LogicalPlan relation = LogicalPlanDSL.relation("index", TestConstants.DUMMY_CATALOG);
    assertEquals(0, relation.getChild().size());
  }

}