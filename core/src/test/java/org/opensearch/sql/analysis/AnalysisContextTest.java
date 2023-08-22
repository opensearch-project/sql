/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class AnalysisContextTest {

  private final AnalysisContext context = new AnalysisContext();

  @Test
  public void rootEnvironmentShouldBeThereInitially() {
    assertNotNull(context.peek());
  }

  @Test
  public void pushAndPopEnvironmentShouldPass() {
    context.push();
    context.pop();
  }

  @Test
  public void popRootEnvironmentShouldPass() {
    context.pop();
  }

  @Test
  public void popEmptyEnvironmentStackShouldFail() {
    context.pop();
    NullPointerException exception = assertThrows(NullPointerException.class, () -> context.pop());
    assertEquals("Fail to pop context due to no environment present", exception.getMessage());
  }
}
