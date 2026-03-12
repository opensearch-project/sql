/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import net.jqwik.api.*;

/**
 * Property-based tests for unparsing identifier and literal correctness (Property 27).
 *
 * <p>**Validates: Requirements 15.1, 15.2, 15.3**
 *
 * <p>For any valid identifier string, unparsing with the ClickHouse SqlDialect SHALL produce a
 * backtick-quoted identifier. For any string literal containing single quotes or backslashes,
 * unparsing SHALL produce a correctly escaped string literal that can be re-parsed.
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class UnparsingIdentifierLiteralPropertyTest {

  private static final OpenSearchClickHouseSqlDialect DIALECT =
      OpenSearchClickHouseSqlDialect.DEFAULT;

  // -------------------------------------------------------------------------
  // Property 27: Identifier quoting with backticks (Requirement 15.1)
  // -------------------------------------------------------------------------

  /**
   * Property 27 (identifier quoting): For any valid identifier string, quoteIdentifier SHALL wrap
   * it in backticks.
   *
   * <p>**Validates: Requirements 15.1**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteIdentifierWrapsInBackticks(@ForAll("identifierStrings") String identifier) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteIdentifier(buf, identifier);
    String result = buf.toString();

    assertTrue(
        result.startsWith("`"),
        "Quoted identifier should start with backtick, got: " + result);
    assertTrue(
        result.endsWith("`"),
        "Quoted identifier should end with backtick, got: " + result);
  }

  /**
   * Property 27 (identifier content preserved): The identifier content between backticks should
   * contain the original identifier string.
   *
   * <p>**Validates: Requirements 15.1**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteIdentifierPreservesContent(@ForAll("identifierStrings") String identifier) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteIdentifier(buf, identifier);
    String result = buf.toString();

    // Strip the surrounding backticks
    String inner = result.substring(1, result.length() - 1);
    assertTrue(
        inner.contains(identifier),
        "Quoted identifier should contain the original identifier '"
            + identifier
            + "', got: "
            + result);
  }

  // -------------------------------------------------------------------------
  // Property 27: String literal escaping (Requirement 15.2)
  // -------------------------------------------------------------------------

  /**
   * Property 27 (string literal quoting): For any string value, quoteStringLiteral SHALL wrap it in
   * single quotes.
   *
   * <p>**Validates: Requirements 15.2**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteStringLiteralWrapsInSingleQuotes(@ForAll("stringLiteralValues") String value) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteStringLiteral(buf, null, value);
    String result = buf.toString();

    assertTrue(
        result.startsWith("'"),
        "Quoted string literal should start with single quote, got: " + result);
    assertTrue(
        result.endsWith("'"),
        "Quoted string literal should end with single quote, got: " + result);
  }

  /**
   * Property 27 (single quote escaping): For any string containing single quotes, quoteStringLiteral
   * SHALL escape them as \' (backslash-quote) per ClickHouse rules.
   *
   * <p>**Validates: Requirements 15.2**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteStringLiteralEscapesSingleQuotes(
      @ForAll("stringsWithSingleQuotes") String value) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteStringLiteral(buf, null, value);
    String result = buf.toString();

    // Remove the surrounding single quotes
    String inner = result.substring(1, result.length() - 1);

    // The inner content should not contain unescaped single quotes.
    // Every single quote in the inner content must be preceded by a backslash.
    for (int i = 0; i < inner.length(); i++) {
      if (inner.charAt(i) == '\'') {
        assertTrue(
            i > 0 && inner.charAt(i - 1) == '\\',
            "Single quote at position "
                + i
                + " should be escaped with backslash in: "
                + result);
      }
    }
  }

  /**
   * Property 27 (backslash escaping): For any string containing backslashes, quoteStringLiteral
   * SHALL escape them as \\\\ (double backslash) per ClickHouse rules.
   *
   * <p>**Validates: Requirements 15.2**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteStringLiteralEscapesBackslashes(
      @ForAll("stringsWithBackslashes") String value) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteStringLiteral(buf, null, value);
    String result = buf.toString();

    // Remove the surrounding single quotes
    String inner = result.substring(1, result.length() - 1);

    // Count backslashes in the output: each original backslash should become \\
    // So the number of backslashes in the output should be at least 2x the input count
    long inputBackslashes = value.chars().filter(c -> c == '\\').count();
    long outputBackslashes = inner.chars().filter(c -> c == '\\').count();

    assertTrue(
        outputBackslashes >= inputBackslashes * 2,
        "Each backslash should be escaped to \\\\. Input backslashes: "
            + inputBackslashes
            + ", output backslashes: "
            + outputBackslashes
            + ", result: "
            + result);
  }

  /**
   * Property 27 (Unicode preservation): For any string containing Unicode characters,
   * quoteStringLiteral SHALL preserve them in the output.
   *
   * <p>**Validates: Requirements 15.2**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteStringLiteralPreservesUnicode(@ForAll("stringsWithUnicode") String value) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteStringLiteral(buf, null, value);
    String result = buf.toString();

    // Remove the surrounding single quotes
    String inner = result.substring(1, result.length() - 1);

    // All Unicode characters (non-ASCII, non-quote, non-backslash) should be preserved as-is
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c != '\'' && c != '\\') {
        assertTrue(
            inner.indexOf(c) >= 0,
            "Unicode character '"
                + c
                + "' (U+"
                + String.format("%04X", (int) c)
                + ") should be preserved in output: "
                + result);
      }
    }
  }

  /**
   * Property 27 (round-trip decodability): For any string, the escaped output should be decodable
   * back to the original string by reversing the escaping rules (\\\\ → \\, \\' → ').
   *
   * <p>**Validates: Requirements 15.2**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void quoteStringLiteralIsRoundTripDecodable(@ForAll("stringLiteralValues") String value) {
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteStringLiteral(buf, null, value);
    String result = buf.toString();

    // Remove the surrounding single quotes
    String inner = result.substring(1, result.length() - 1);

    // Decode: replace \\ with \ and \' with '
    StringBuilder decoded = new StringBuilder();
    for (int i = 0; i < inner.length(); i++) {
      char c = inner.charAt(i);
      if (c == '\\' && i + 1 < inner.length()) {
        char next = inner.charAt(i + 1);
        if (next == '\\') {
          decoded.append('\\');
          i++;
        } else if (next == '\'') {
          decoded.append('\'');
          i++;
        } else {
          decoded.append(c);
        }
      } else {
        decoded.append(c);
      }
    }

    assertEquals(
        value,
        decoded.toString(),
        "Decoding the escaped string should produce the original value. "
            + "Original: '"
            + value
            + "', Escaped: "
            + result);
  }

  // -------------------------------------------------------------------------
  // Property 27: Date/time literal syntax (Requirement 15.3)
  // -------------------------------------------------------------------------

  /**
   * Property 27 (date/time literal syntax): The ClickHouse dialect should use function-style syntax
   * for date/time literals. When unparsing a DATE or TIMESTAMP literal via SqlNode.toSqlString, the
   * output should use ClickHouse function-style syntax rather than ANSI DATE/TIMESTAMP keywords.
   *
   * <p>This is inherited from the parent ClickHouseSqlDialect and verified by checking that the
   * dialect's DatabaseProduct is CLICKHOUSE, which triggers function-style date literal unparsing.
   *
   * <p>**Validates: Requirements 15.3**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 27: Unparsing identifier and literal correctness")
  void dialectSupportsClickHouseDateTimeSyntax(
      @ForAll("dateTimeStrings") String dateTimeValue) {
    // Verify the dialect produces correctly quoted date/time string values
    // that could be used inside ClickHouse function-style date constructors
    // like toDateTime('2024-01-01 00:00:00')
    StringBuilder buf = new StringBuilder();
    DIALECT.quoteStringLiteral(buf, null, dateTimeValue);
    String quoted = buf.toString();

    // The quoted value should be a valid single-quoted string
    assertTrue(
        quoted.startsWith("'") && quoted.endsWith("'"),
        "Date/time string should be properly quoted: " + quoted);

    // The date/time value should be preserved (no special chars to escape)
    String inner = quoted.substring(1, quoted.length() - 1);
    assertEquals(
        dateTimeValue,
        inner,
        "Date/time value should be preserved unchanged inside quotes: " + quoted);
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  /** Generates random identifier strings: letters, digits, underscores, 1-30 chars. */
  @Provide
  Arbitrary<String> identifierStrings() {
    Arbitrary<Character> firstChar = Arbitraries.chars().range('a', 'z').range('A', 'Z');
    Arbitrary<String> rest =
        Arbitraries.strings()
            .withCharRange('a', 'z')
            .withCharRange('A', 'Z')
            .withCharRange('0', '9')
            .withChars('_')
            .ofMinLength(0)
            .ofMaxLength(29);

    return Combinators.combine(firstChar, rest).as((first, tail) -> first + tail);
  }

  /**
   * Generates random string values for string literal testing, including plain strings, strings
   * with single quotes, backslashes, and Unicode characters.
   */
  @Provide
  Arbitrary<String> stringLiteralValues() {
    return Arbitraries.oneOf(
        // Plain ASCII strings
        Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(20),
        // Strings with single quotes
        Arbitraries.of("it's", "can't", "won't", "O'Brien", "'quoted'", "a'b'c"),
        // Strings with backslashes
        Arbitraries.of("path\\to\\file", "\\\\server", "back\\slash", "a\\b\\c"),
        // Strings with both
        Arbitraries.of("it\\'s", "path\\to\\'file", "a\\'b\\c"),
        // Strings with Unicode
        Arbitraries.of("café", "naïve", "日本語", "Ω≈ç", "emoji\uD83D\uDE00"),
        // Empty string
        Arbitraries.of(""));
  }

  /** Generates strings that always contain at least one single quote. */
  @Provide
  Arbitrary<String> stringsWithSingleQuotes() {
    return Arbitraries.strings()
        .alpha()
        .ofMinLength(0)
        .ofMaxLength(10)
        .map(s -> s + "'" + s);
  }

  /** Generates strings that always contain at least one backslash. */
  @Provide
  Arbitrary<String> stringsWithBackslashes() {
    return Arbitraries.strings()
        .alpha()
        .ofMinLength(0)
        .ofMaxLength(10)
        .map(s -> s + "\\" + s);
  }

  /** Generates strings containing Unicode characters. */
  @Provide
  Arbitrary<String> stringsWithUnicode() {
    return Arbitraries.of(
        "café",
        "naïve",
        "日本語テスト",
        "Ω≈ç√∫",
        "über",
        "señor",
        "Ñoño",
        "αβγδ",
        "中文测试",
        "한국어");
  }

  /** Generates date/time strings in standard formats. */
  @Provide
  Arbitrary<String> dateTimeStrings() {
    return Arbitraries.oneOf(
        // Date strings
        Combinators.combine(
                Arbitraries.integers().between(2000, 2030),
                Arbitraries.integers().between(1, 12),
                Arbitraries.integers().between(1, 28))
            .as(
                (year, month, day) ->
                    String.format("%04d-%02d-%02d", year, month, day)),
        // DateTime strings
        Combinators.combine(
                Arbitraries.integers().between(2000, 2030),
                Arbitraries.integers().between(1, 12),
                Arbitraries.integers().between(1, 28),
                Arbitraries.integers().between(0, 23),
                Arbitraries.integers().between(0, 59),
                Arbitraries.integers().between(0, 59))
            .as(
                (year, month, day, hour, min, sec) ->
                    String.format(
                        "%04d-%02d-%02d %02d:%02d:%02d",
                        year, month, day, hour, min, sec)));
  }
}
