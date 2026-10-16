/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.exception.SemanticCheckException;

class OpenSearchRelRunnersTest {

  /** Calcite keeps an implementor's exception as suppressed by its "Unable to implement". */
  @Test
  void findsAUserErrorAmongSuppressedExceptions() {
    SemanticCheckException userError = new SemanticCheckException("knn is only supported ...");
    IllegalStateException unableToImplement = new IllegalStateException("Unable to implement");
    unableToImplement.addSuppressed(userError);
    SQLException preparation = new SQLException("Error while preparing plan", unableToImplement);

    assertSame(userError, OpenSearchRelRunners.findSemanticError(preparation));
  }

  @Test
  void stopsAtACycle() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second", first);
    first.addSuppressed(second);

    assertNull(OpenSearchRelRunners.findSemanticError(first));
  }
}
