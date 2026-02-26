/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import net.jqwik.api.*;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Property-based tests for backtick quoting equivalence (Property 4). Validates: Requirements 4.2
 *
 * <p>For any valid identifier string, a query using backtick-quoted identifiers SHALL parse to the
 * same SqlNode AST as the same query using double-quoted identifiers (when the dialect's parser
 * config uses backtick quoting).
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class BacktickQuotingEquivalencePropertyTest {

  /** ClickHouse dialect parser config: backtick quoting, case insensitive, TO_LOWER. */
  private static final SqlParser.Config BACKTICK_CONFIG =
      ClickHouseDialectPlugin.INSTANCE.parserConfig();

  /**
   * Equivalent config using double-quote quoting (Calcite default) with the same case sensitivity
   * settings.
   */
  private static final SqlParser.Config DOUBLE_QUOTE_CONFIG =
      SqlParser.config()
          .withQuoting(Quoting.DOUBLE_QUOTE)
          .withCaseSensitive(false)
          .withUnquotedCasing(Casing.TO_LOWER);

  // -------------------------------------------------------------------------
  // Property 4: Backtick quoting equivalence
  // -------------------------------------------------------------------------

  /**
   * Property 4: Backtick quoting equivalence — For any valid identifier string, a query using
   * backtick-quoted identifiers SHALL parse to the same SqlNode AST as the same query using
   * double-quoted identifiers (when the dialect's parser config uses backtick quoting).
   *
   * <p>Validates: Requirements 4.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 4: Backtick quoting equivalence")
  void backtickQuotedIdentifierParsesToSameAstAsDoubleQuoted(
      @ForAll("validIdentifiers") String identifier) throws SqlParseException {
    String backtickQuery = "SELECT `" + identifier + "` FROM t";
    String doubleQuoteQuery = "SELECT \"" + identifier + "\" FROM t";

    SqlNode backtickAst = parseSql(backtickQuery, BACKTICK_CONFIG);
    SqlNode doubleQuoteAst = parseSql(doubleQuoteQuery, DOUBLE_QUOTE_CONFIG);

    assertEquals(
        doubleQuoteAst.toString(),
        backtickAst.toString(),
        "Backtick-quoted query AST should match double-quoted query AST. "
            + "Identifier: '"
            + identifier
            + "', Backtick query: '"
            + backtickQuery
            + "', Double-quote query: '"
            + doubleQuoteQuery
            + "'");
  }

  /**
   * Property 4 (WHERE clause): Backtick and double-quote quoting should produce the same AST when
   * identifiers appear in WHERE clauses.
   *
   * <p>Validates: Requirements 4.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 4: Backtick quoting equivalence")
  void backtickQuotingEquivalenceInWhereClause(
      @ForAll("validIdentifiers") String identifier) throws SqlParseException {
    String backtickQuery = "SELECT `" + identifier + "` FROM t WHERE `" + identifier + "` > 0";
    String doubleQuoteQuery =
        "SELECT \"" + identifier + "\" FROM t WHERE \"" + identifier + "\" > 0";

    SqlNode backtickAst = parseSql(backtickQuery, BACKTICK_CONFIG);
    SqlNode doubleQuoteAst = parseSql(doubleQuoteQuery, DOUBLE_QUOTE_CONFIG);

    assertEquals(
        doubleQuoteAst.toString(),
        backtickAst.toString(),
        "Backtick-quoted WHERE clause AST should match double-quoted. "
            + "Identifier: '"
            + identifier
            + "'");
  }

  /**
   * Property 4 (multiple identifiers): Backtick and double-quote quoting should produce the same
   * AST when multiple quoted identifiers appear in the same query.
   *
   * <p>Validates: Requirements 4.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 4: Backtick quoting equivalence")
  void backtickQuotingEquivalenceWithMultipleIdentifiers(
      @ForAll("validIdentifiers") String id1, @ForAll("validIdentifiers") String id2)
      throws SqlParseException {
    String backtickQuery = "SELECT `" + id1 + "`, `" + id2 + "` FROM t";
    String doubleQuoteQuery = "SELECT \"" + id1 + "\", \"" + id2 + "\" FROM t";

    SqlNode backtickAst = parseSql(backtickQuery, BACKTICK_CONFIG);
    SqlNode doubleQuoteAst = parseSql(doubleQuoteQuery, DOUBLE_QUOTE_CONFIG);

    assertEquals(
        doubleQuoteAst.toString(),
        backtickAst.toString(),
        "Multiple backtick-quoted identifiers AST should match double-quoted. "
            + "Identifiers: '"
            + id1
            + "', '"
            + id2
            + "'");
  }

  /**
   * Property 4 (ORDER BY): Backtick and double-quote quoting should produce the same AST when
   * identifiers appear in ORDER BY clauses.
   *
   * <p>Validates: Requirements 4.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 4: Backtick quoting equivalence")
  void backtickQuotingEquivalenceInOrderBy(
      @ForAll("validIdentifiers") String identifier) throws SqlParseException {
    String backtickQuery = "SELECT `" + identifier + "` FROM t ORDER BY `" + identifier + "`";
    String doubleQuoteQuery =
        "SELECT \"" + identifier + "\" FROM t ORDER BY \"" + identifier + "\"";

    SqlNode backtickAst = parseSql(backtickQuery, BACKTICK_CONFIG);
    SqlNode doubleQuoteAst = parseSql(doubleQuoteQuery, DOUBLE_QUOTE_CONFIG);

    assertEquals(
        doubleQuoteAst.toString(),
        backtickAst.toString(),
        "Backtick-quoted ORDER BY AST should match double-quoted. "
            + "Identifier: '"
            + identifier
            + "'");
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  /**
   * Generates valid SQL identifiers: start with a letter, followed by alphanumeric characters and
   * underscores. Length between 1 and 20 characters.
   */
  @Provide
  Arbitrary<String> validIdentifiers() {
    Arbitrary<Character> firstChar = Arbitraries.chars().range('a', 'z').range('A', 'Z');
    Arbitrary<String> rest =
        Arbitraries.strings()
            .withCharRange('a', 'z')
            .withCharRange('A', 'Z')
            .withCharRange('0', '9')
            .withChars('_')
            .ofMinLength(0)
            .ofMaxLength(19);

    return Combinators.combine(firstChar, rest).as((first, tail) -> first + tail);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static SqlNode parseSql(String sql, SqlParser.Config config) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, config);
    return parser.parseQuery();
  }
}
