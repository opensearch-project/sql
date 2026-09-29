/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprType;

/**
 * Index-mapping-aware emission for {@link SearchLiteral} and {@link SearchComparison}.
 *
 * <p>On <b>text</b>, the user's quoting is honored: unquoted keeps {@code *}/{@code ?} as
 * query_string operators, quoted becomes a phrase. On the <b>keyword family</b>, quoting is
 * irrelevant (the analyzer is a no-op) so we emit whole-value semantics — a wildcard pattern when
 * the value holds an unescaped wildcard, otherwise an exact term. Other mappings keep legacy
 * behavior.
 */
class SearchLiteralTest {

  /** Stub {@link ExprType} reporting a given legacyTypeName — enough for the mapping checks. */
  private static ExprType typeOf(String legacyName) {
    return new ExprType() {
      @Override
      public String typeName() {
        return legacyName;
      }

      @Override
      public String legacyTypeName() {
        return legacyName;
      }
    };
  }

  private static final ExprType KEYWORD = typeOf("KEYWORD");
  private static final ExprType CONSTANT_KEYWORD = typeOf("CONSTANT_KEYWORD");
  private static final ExprType TEXT = typeOf("TEXT");
  private static final ExprType MATCH_ONLY_TEXT = typeOf("MATCH_ONLY_TEXT");
  private static final ExprType DATE = typeOf("TIMESTAMP");
  private static final ExprType LONG = typeOf("LONG");

  /** Value the user wrote inside quotes. */
  private static SearchLiteral q(String value) {
    return new SearchLiteral(new Literal(value, DataType.STRING), true);
  }

  /** Value the user wrote bare. */
  private static SearchLiteral bare(String value) {
    return new SearchLiteral(new Literal(value, DataType.STRING), false);
  }

  // -------------------------------------------------------------------------
  // TEXT: honor the user's quoting.
  // -------------------------------------------------------------------------

  @Test
  void text_quoted_emits_phrase() {
    assertEquals("\"foo bar\"", q("foo bar").toQueryString(TEXT));
  }

  @Test
  void text_unquoted_emits_bare_term() {
    assertEquals("foo", bare("foo").toQueryString(TEXT));
  }

  @Test
  void text_quoted_wildcard_term_stays_unquoted() {
    // Whitespace-free value with a wildcard: emitted unquoted so '*' keeps operator meaning.
    // Quoting it would let the analyzer discard the wildcard.
    assertEquals("foo*", q("foo*").toQueryString(TEXT));
    assertEquals("f?o", q("f?o").toQueryString(TEXT));
  }

  @Test
  void text_quoted_wildcard_with_whitespace_emits_phrase() {
    // With whitespace, unquoted would split into separate clauses and the tail would lose its
    // field binding, so the phrase form is kept.
    assertEquals("\"foo bar*\"", q("foo bar*").toQueryString(TEXT));
    assertEquals("\"*foo bar\"", q("*foo bar").toQueryString(TEXT));
  }

  @Test
  void text_escaped_wildcard_is_not_a_wildcard_term() {
    // '*' is escaped, so it is a literal — the value has no unescaped wildcard and stays a phrase.
    assertEquals("\"foo\\*\"", q("foo\\*").toQueryString(TEXT));
  }

  @Test
  void text_unquoted_wildcard_keeps_operator() {
    assertEquals("foo*", bare("foo*").toQueryString(TEXT));
  }

  @Test
  void text_match_only_text_behaves_like_text() {
    assertEquals("\"foo bar\"", q("foo bar").toQueryString(MATCH_ONLY_TEXT));
    assertEquals("foo*", bare("foo*").toQueryString(MATCH_ONLY_TEXT));
  }

  /**
   * A quoted value the analyzer splits must not become an OR of tokens. These values hold no
   * whitespace, so before the mapping split they were emitted unquoted.
   */
  @Test
  void text_quoted_value_the_analyzer_splits_emits_phrase() {
    assertEquals("\"foo\\-bar\"", q("foo-bar").toQueryString(TEXT));
    assertEquals("\"foo=bar\"", q("foo=bar").toQueryString(TEXT));
  }

  // -------------------------------------------------------------------------
  // KEYWORD FAMILY: quoting is irrelevant; wildcard presence selects the form.
  // -------------------------------------------------------------------------

  @Test
  void keyword_no_wildcard_keeps_legacy_emission() {
    // Quoting carries no information on keyword — a quoted phrase and a bare term both resolve to
    // the same single term — so the emission is left exactly as it was.
    assertEquals("foo", bare("foo").toQueryString(KEYWORD));
    assertEquals("foo", q("foo").toQueryString(KEYWORD));
    assertEquals("foo\\-bar", q("foo-bar").toQueryString(KEYWORD));
    assertEquals("\"foo bar\"", q("foo bar").toQueryString(KEYWORD));
  }

  @Test
  void keyword_no_wildcard_quoting_is_irrelevant() {
    assertEquals(q("foo-bar").toQueryString(KEYWORD), bare("foo-bar").toQueryString(KEYWORD));
  }

  @Test
  void keyword_wildcard_with_space_escapes_the_space() {
    // The reported bug (#5682): whole-value pattern, not a phrase with a literal '*'.
    assertEquals("foo\\ bar*", q("foo bar*").toQueryString(KEYWORD));
  }

  @Test
  void keyword_wildcard_without_space_emits_bare_pattern() {
    assertEquals("foo*", q("foo*").toQueryString(KEYWORD));
    assertEquals("foo*", bare("foo*").toQueryString(KEYWORD));
  }

  @Test
  void keyword_leading_and_interior_wildcards_escape_spaces() {
    assertEquals("*foo\\ bar", q("*foo bar").toQueryString(KEYWORD));
    assertEquals("foo\\ *baz", q("foo *baz").toQueryString(KEYWORD));
    assertEquals("foo\\ b?r", q("foo b?r").toQueryString(KEYWORD));
  }

  @Test
  void keyword_wildcard_with_special_chars_escapes_all() {
    assertEquals(
        "POST\\ \\/test\\-logs\\/_search*", q("POST /test-logs/_search*").toQueryString(KEYWORD));
  }

  @Test
  void keyword_escaped_wildcard_is_an_exact_term() {
    // The user escaped '*', so it is a literal — no wildcard, so the exact-term form applies.
    assertEquals("\"foo \\*\"", q("foo \\*").toQueryString(KEYWORD));
  }

  @Test
  void constant_keyword_behaves_like_keyword() {
    assertEquals("\"foo bar\"", q("foo bar").toQueryString(CONSTANT_KEYWORD));
    assertEquals("foo\\ bar*", q("foo bar*").toQueryString(CONSTANT_KEYWORD));
  }

  // -------------------------------------------------------------------------
  // OTHER MAPPINGS + unresolved: legacy behavior (space check), untouched.
  // -------------------------------------------------------------------------

  @Test
  void date_field_keeps_legacy_unquoted_form() {
    // Hyphens are escaped by the legacy path too — this is unchanged from before the mapping
    // split, and query_string still parses the value as a date.
    assertEquals("2024\\-01\\-15", q("2024-01-15").toQueryString(DATE));
  }

  @Test
  void numeric_field_keeps_legacy_unquoted_form() {
    assertEquals("not\\-a\\-number", q("not-a-number").toQueryString(LONG));
  }

  @Test
  void unresolved_field_keeps_legacy_space_check() {
    assertEquals("\"foo bar*\"", q("foo bar*").toQueryString((ExprType) null));
    assertEquals("foo*", q("foo*").toQueryString((ExprType) null));
  }

  @Test
  void argless_toQueryString_matches_null_type_branch() {
    SearchLiteral lit = q("foo bar*");
    assertEquals(lit.toQueryString((ExprType) null), lit.toQueryString());
  }

  // -------------------------------------------------------------------------
  // End-to-end via SearchComparison + resolver — the actual planner call path.
  // -------------------------------------------------------------------------

  private static SearchComparison cmp(String field, SearchLiteral value) {
    return new SearchComparison(
        new Field(new QualifiedName(field), List.of()), SearchComparison.Operator.EQUALS, value);
  }

  @Test
  void comparison_emits_wildcard_pattern_on_keyword() {
    Function<String, ExprType> resolver = Map.of("name", KEYWORD)::get;
    assertEquals("name:foo\\ bar*", cmp("name", q("foo bar*")).toQueryString(resolver));
  }

  @Test
  void comparison_emits_phrase_on_text_when_quoted() {
    Function<String, ExprType> resolver = Map.of("body", TEXT)::get;
    assertEquals("body:\"foo=bar\"", cmp("body", q("foo=bar")).toQueryString(resolver));
  }

  @Test
  void comparison_emits_bare_term_on_text_when_unquoted() {
    Function<String, ExprType> resolver = Map.of("body", TEXT)::get;
    assertEquals("body:foo*", cmp("body", bare("foo*")).toQueryString(resolver));
  }

  @Test
  void comparison_unknown_field_falls_back_to_legacy_form() {
    Function<String, ExprType> resolver = f -> null;
    assertEquals("unmapped:\"foo bar*\"", cmp("unmapped", q("foo bar*")).toQueryString(resolver));
  }
}
