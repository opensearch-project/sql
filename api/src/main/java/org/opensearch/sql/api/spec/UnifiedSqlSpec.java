/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.opensearch.sql.api.spec.search.SearchExtension;

/**
 * SQL language specification. Configures Calcite's parser, validator, and composable extensions for
 * OpenSearch SQL compatibility.
 *
 * <p>Use {@link #extended()} for the default configuration with lenient syntax, hyphenated
 * identifiers, and search functions.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
public class UnifiedSqlSpec implements LanguageSpec {

  /** Lexical rules: identifier quoting, character escaping, and special identifier support. */
  private final Lex lex;

  /** Parser implementation: controls keyword reservation and grammar extensions. */
  private final SqlParserImplFactory parserFactory;

  /** Validation rules: what SQL semantics the validator accepts (GROUP BY, LIMIT, coercion). */
  private final SqlConformanceEnum conformance;

  /** Composable extensions contributing operators and post-parse rewrite rules. */
  @Getter private final List<LanguageExtension> extensions;

  /**
   * Extended SQL spec: Babel parser, BIG_QUERY lex (hyphenated identifiers, backtick quoting),
   * BABEL conformance (lenient GROUP BY, LIMIT, optional FROM), and search functions.
   */
  public static UnifiedSqlSpec extended() {
    return new UnifiedSqlSpec(
        Lex.BIG_QUERY,
        SqlBabelParserImpl.FACTORY,
        SqlConformanceEnum.BABEL,
        List.of(new SearchExtension()));
  }

  @Override
  public SqlParser.Config parserConfig() {
    return SqlParser.config()
        .withParserFactory(parserFactory)
        .withLex(lex)
        .withConformance(conformance);
  }

  @Override
  public SqlValidator.Config validatorConfig() {
    return SqlValidator.Config.DEFAULT.withConformance(conformance);
  }
}
