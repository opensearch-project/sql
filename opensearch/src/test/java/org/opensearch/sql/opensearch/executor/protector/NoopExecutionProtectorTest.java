/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@ExtendWith(MockitoExtension.class)
class NoopExecutionProtectorTest {

  @Mock private PhysicalPlan plan;

  @Test
  void protect() {
    NoopExecutionProtector executionProtector = new NoopExecutionProtector();
    PhysicalPlan protectedPlan = executionProtector.protect(plan);

    assertEquals(plan, protectedPlan);
  }
}
