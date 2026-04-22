/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SyntaxCheckException extends RuntimeException {

  // Structured context for response formatting
  private String offendingToken;
  private Integer line;
  private Integer charPosition;
  private String errorContext;
  private Integer tokenStart;
  private Integer tokenEnd;

  public SyntaxCheckException(String message) {
    super(message);
  }
}
