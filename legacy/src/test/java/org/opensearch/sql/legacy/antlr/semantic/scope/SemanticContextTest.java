/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

import org.junit.Assert;
import org.junit.Test;

/** Test cases for semantic context */
public class SemanticContextTest {

  private final SemanticContext context = new SemanticContext();

  @Test
  public void rootEnvironmentShouldBeThereInitially() {
    Assert.assertNotNull(
        "Didn't find root environment. Context is NOT supposed to be empty initially",
        context.peek());
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

  @Test(expected = NullPointerException.class)
  public void popEmptyEnvironmentStackShouldFail() {
    context.pop();
    context.pop();
  }
}
