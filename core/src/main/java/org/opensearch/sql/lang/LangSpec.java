/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.lang;

import static org.opensearch.sql.executor.QueryType.PPL;

import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.QueryType;

/**
 * Represents a language specification for query processing.
 *
 * <p>This interface defines basic methods for language-specific behaviors, such as determining the
 * language type and mapping expression types to type names. Two language specifications are
 * provided: SQL and PPL.
 */
public interface LangSpec {

  /** The default SQL language specification instance. */
  LangSpec SQL_SPEC = new LangSpec() {};

  /**
   * Returns a language specification instance based on the provided language name.
   *
   * @param language the name of the language, case-insensitive.
   * @return the PPL language specification if the language is PPL (ignoring case); otherwise, the
   *     SQL language specification.
   */
  static LangSpec fromLanguage(String language) {
    if (PPL.name().equalsIgnoreCase(language)) {
      return PPLLangSpec.PPL_SPEC;
    } else {
      return SQL_SPEC;
    }
  }

  /**
   * Returns the language type of this specification.
   *
   * <p>By default, the language is considered SQL.
   *
   * @return the language type, SQL by default.
   */
  default QueryType language() {
    return QueryType.SQL;
  }

  /**
   * Returns the type name for the given expression type.
   *
   * <p>This default implementation returns the result of {@code exprType.typeName()}.
   *
   * @param exprType the expression type.
   * @return the type name of the expression.
   */
  default String typeName(ExprType exprType) {
    return exprType.typeName();
  }
}
