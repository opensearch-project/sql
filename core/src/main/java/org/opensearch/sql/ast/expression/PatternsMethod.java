package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum PatternsMethod {
  REGEX("regex"),
  PUNCT("punct");

  @Getter
  private final String name;
}
