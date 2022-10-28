/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch;

import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;

public class OpenSearchTestBase {

  protected static DSL dsl;

  {
    ExpressionConfig config = new ExpressionConfig();
    dsl = config.dsl(config.functionRepository(config.functionExecutionContext()));
  }
}
