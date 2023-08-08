/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
class PlanContextTest {

  @Mock private Split split;

  @Test
  void createEmptyPlanContext() {
    assertTrue(PlanContext.emptyPlanContext().getSplit().isEmpty());
  }

  @Test
  void createPlanContextWithSplit() {
    assertTrue(new PlanContext(split).getSplit().isPresent());
  }
}
