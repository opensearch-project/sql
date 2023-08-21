/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.opensearch.sql.legacy.antlr.SqlAnalysisException;

/** Exception for semantic analysis */
public class SemanticAnalysisException extends SqlAnalysisException {

  public SemanticAnalysisException(String message) {
    super(message);
  }
}
