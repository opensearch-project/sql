/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.syntax;

import org.opensearch.sql.legacy.antlr.SqlAnalysisException;

/** Exception for syntax analysis */
public class SyntaxAnalysisException extends SqlAnalysisException {

  public SyntaxAnalysisException(String message) {
    super(message);
  }
}
