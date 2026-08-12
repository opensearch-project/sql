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
 * Field-type-aware emission for {@link SearchLiteral} and {@link SearchComparison}.
 *
 * <p>Space + unescaped wildcard on a keyword field must not be phrase-quoted (the phrase form
 * silently strips wildcard semantics inside quotes on keyword). On text (or when the type is
 * unknown), the legacy phrase form is preserved to avoid regressing today's behavior — see the
 * repro matrix in issue #5682.
 */
class SearchLiteralTest {

  /** Stub {@link ExprType} that reports a given legacyTypeName — enough for isTextLike(). */
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
  private static final ExprType TEXT = typeOf("TEXT");
  private static final ExprType MATCH_ONLY_TEXT = typeOf("MATCH_ONLY_TEXT");

  private static SearchLiteral phrase(String value) {
    return new SearchLiteral(new Literal(value, DataType.STRING), true);
  }

  private static SearchLiteral bare(String value) {
    return new SearchLiteral(new Literal(value, DataType.STRING), false);
  }

  // -------------------------------------------------------------------------
  // Phrase without wildcard: unchanged on both field types (regression guard).
  // -------------------------------------------------------------------------

  @Test
  void phrase_no_wildcard_keyword_stays_quoted() {
    assertEquals("\"foo bar\"", phrase("foo bar").toQueryString(KEYWORD));
  }

  @Test
  void phrase_no_wildcard_text_stays_quoted() {
    assertEquals("\"foo bar\"", phrase("foo bar").toQueryString(TEXT));
  }

  // -------------------------------------------------------------------------
  // Phrase with unescaped wildcard on keyword: emit escaped-space wildcard term.
  // These are the D-family rows fixed by issue #5682.
  // -------------------------------------------------------------------------

  @Test
  void phrase_trailing_wildcard_keyword_emits_escaped_space_prefix() {
    // P6-k: name="foo bar*" → PrefixQuery on whole keyword term
    assertEquals("foo\\ bar*", phrase("foo bar*").toQueryString(KEYWORD));
  }

  @Test
  void phrase_leading_wildcard_keyword_emits_escaped_space_wildcard() {
    // L3-k: name="*foo bar"
    assertEquals("*foo\\ bar", phrase("*foo bar").toQueryString(KEYWORD));
  }

  @Test
  void phrase_interior_wildcard_keyword_emits_escaped_space_wildcard() {
    // I3-k: name="foo *baz"
    assertEquals("foo\\ *baz", phrase("foo *baz").toQueryString(KEYWORD));
  }

  @Test
  void phrase_question_wildcard_keyword_emits_escaped_space_wildcard() {
    // Q5-k: name="foo b?r"
    assertEquals("foo\\ b?r", phrase("foo b?r").toQueryString(KEYWORD));
  }

  @Test
  void phrase_with_special_chars_and_wildcard_keyword_escapes_all() {
    // D3-k repro: name="POST /test-logs/_search*" → PrefixQuery, special chars still escaped.
    assertEquals(
        "POST\\ \\/test\\-logs\\/_search*",
        phrase("POST /test-logs/_search*").toQueryString(KEYWORD));
  }

  // -------------------------------------------------------------------------
  // Phrase with unescaped wildcard on text / match_only_text / unknown: keep
  // legacy phrase form (no regression on text; wildcard silently ignored by
  // Lucene inside phrase, same as today).
  // -------------------------------------------------------------------------

  @Test
  void phrase_wildcard_text_keeps_phrase_form() {
    assertEquals("\"foo bar*\"", phrase("foo bar*").toQueryString(TEXT));
  }

  @Test
  void phrase_wildcard_match_only_text_keeps_phrase_form() {
    assertEquals("\"foo bar*\"", phrase("foo bar*").toQueryString(MATCH_ONLY_TEXT));
  }

  @Test
  void phrase_wildcard_unknown_type_keeps_phrase_form() {
    // Unknown → treat as text-like so we never regress an unresolvable field.
    assertEquals("\"foo bar*\"", phrase("foo bar*").toQueryString((ExprType) null));
  }

  // -------------------------------------------------------------------------
  // Phrase with escaped wildcard: user asked for a LITERAL '*'/'?' — keep the
  // phrase form even on keyword. The isPhrase flag was set for a reason.
  // -------------------------------------------------------------------------

  @Test
  void phrase_escaped_wildcard_keyword_keeps_phrase_form() {
    // Input string literally contains `foo \*` (backslash + '*'). hasUnescapedWildcard() sees the
    // backslash and skips '*', so we treat this as a phrase with a literal '*' — no emission
    // change. QueryStringUtils.escapeLuceneSpecialCharacters keeps '\\' and '*' untouched.
    assertEquals("\"foo \\*\"", phrase("foo \\*").toQueryString(KEYWORD));
  }

  // -------------------------------------------------------------------------
  // Non-phrase (no space): field type does not matter — legacy code path.
  // -------------------------------------------------------------------------

  @Test
  void bare_wildcard_keyword_emits_unquoted() {
    // P1: name="foo*" — never a phrase, works today.
    assertEquals("foo*", bare("foo*").toQueryString(KEYWORD));
  }

  @Test
  void bare_wildcard_text_emits_unquoted() {
    assertEquals("foo*", bare("foo*").toQueryString(TEXT));
  }

  // -------------------------------------------------------------------------
  // Backwards-compat: arg-less toQueryString() must match the null-type branch
  // (legacy phrase form) so callers that never wire the resolver see no change.
  // -------------------------------------------------------------------------

  @Test
  void argless_toQueryString_matches_null_type_branch() {
    SearchLiteral lit = phrase("foo bar*");
    assertEquals(lit.toQueryString((ExprType) null), lit.toQueryString());
  }

  // -------------------------------------------------------------------------
  // End-to-end via SearchComparison + resolver — the actual planner call path.
  // -------------------------------------------------------------------------

  @Test
  void comparison_resolves_field_type_and_emits_wildcard_on_keyword() {
    // name="foo bar*" on a keyword field → name:foo\ bar*
    SearchComparison cmp =
        new SearchComparison(
            new Field(new QualifiedName("name"), List.of()),
            SearchComparison.Operator.EQUALS,
            phrase("foo bar*"));
    Function<String, ExprType> resolver = Map.of("name", KEYWORD)::get;
    assertEquals("name:foo\\ bar*", cmp.toQueryString(resolver));
  }

  @Test
  void comparison_resolves_field_type_and_keeps_phrase_on_text() {
    SearchComparison cmp =
        new SearchComparison(
            new Field(new QualifiedName("name"), List.of()),
            SearchComparison.Operator.EQUALS,
            phrase("foo bar*"));
    Function<String, ExprType> resolver = Map.of("name", TEXT)::get;
    assertEquals("name:\"foo bar*\"", cmp.toQueryString(resolver));
  }

  @Test
  void comparison_unknown_field_falls_back_to_phrase_form() {
    SearchComparison cmp =
        new SearchComparison(
            new Field(new QualifiedName("unmapped"), List.of()),
            SearchComparison.Operator.EQUALS,
            phrase("foo bar*"));
    // Resolver returns null for unknown fields.
    Function<String, ExprType> resolver = f -> null;
    assertEquals("unmapped:\"foo bar*\"", cmp.toQueryString(resolver));
  }
}
