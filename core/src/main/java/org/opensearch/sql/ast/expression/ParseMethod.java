/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum ParseMethod {
  REGEX("regex"),
  GROK("grok"),
  PATTERNS("patterns");

  @Getter
  private final String name;
}
