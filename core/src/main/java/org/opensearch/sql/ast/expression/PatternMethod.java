/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum PatternMethod {
  SIMPLE_PATTERN("simple_pattern"),
  BRAIN("brain");

  @Getter final String name;
}
