/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.junit.Test;

public class SemanticAnalyzerConstantTest extends SemanticAnalyzerTestBase {

  @Test
  public void useNegativeIntegerShouldPass() {
    validate("SELECT * FROM test WHERE age > -1");
  }

  @Test
  public void useNegativeFloatingPointNumberShouldPass() {
    validate("SELECT * FROM test WHERE balance > -1.23456");
  }
}
