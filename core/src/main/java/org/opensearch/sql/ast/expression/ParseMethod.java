package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum ParseMethod {
  REGEX("regex"),
  GROK("grok"),
  PUNCT("punct");

  @Getter
  private final String name;
}
