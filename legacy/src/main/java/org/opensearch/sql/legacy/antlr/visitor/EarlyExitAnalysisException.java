/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.visitor;

/** Exit visitor early due to some reason. */
public class EarlyExitAnalysisException extends RuntimeException {

  public EarlyExitAnalysisException(String message) {
    super(message);
  }
}
