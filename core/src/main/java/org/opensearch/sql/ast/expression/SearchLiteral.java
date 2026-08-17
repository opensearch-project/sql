/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.utils.QueryStringUtils;

/** Search expression for standalone literals. */
@Getter
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchLiteral extends SearchExpression {

  private final UnresolvedExpression literal;

  /**
   * Whether the user wrote this value inside quotes in the PPL query. On a text field this is the
   * user's explicit request for phrase semantics — see {@link #toQueryString(ExprType)}.
   */
  private final boolean userQuoted;

  @Override
  public String toQueryString(Function<String, ExprType> fieldTypeResolver) {
    // Unfielded literal: no enclosing field, so no index type. Take the field-agnostic branch.
    return toQueryString((ExprType) null);
  }

  /**
   * Emits the query_string form for a literal on the RHS of {@link SearchComparison} or inside
   * {@link SearchIn}. Emission is driven by the enclosing field's index mapping, because the
   * mapping decides whether the value gets analyzed:
   *
   * <ul>
   *   <li><b>keyword family with a wildcard</b> — the analyzer is a no-op, so the value has to
   *       reach Lucene as one term for the pattern to apply to the whole stored value. Emitted
   *       unquoted with whitespace escaped.
   *   <li><b>text</b> — honor the user's quoting. Unquoted passes through, so {@code *} and {@code
   *       ?} stay query_string operators. Quoted becomes a phrase, which is how a user asks for
   *       "this whole value, in order" against the analyzed tokens.
   *   <li><b>everything else</b> — keyword without a wildcard, plus date, numeric, ip, boolean and
   *       unresolved fields. Legacy behavior. Note that quoting genuinely carries no information on
   *       keyword: with a no-op analyzer a quoted phrase and a bare term both resolve to the same
   *       single term, so there is nothing to gain by rewriting the emission here.
   * </ul>
   *
   * @param indexType the enclosing field's OpenSearch index-mapping type, or null if unresolved
   */
  public String toQueryString(ExprType indexType) {
    if (literal instanceof Literal) {
      Literal lit = (Literal) literal;
      Object val = lit.getValue();

      // Numbers don't need escaping
      if (val instanceof Number) {
        return val.toString();
      }

      // Strings
      if (val instanceof String) {
        String str = (String) val;

        // A keyword-family field carries whole-value semantics, so a value holding a wildcard has
        // to reach Lucene as a single term. Escaping the whitespace keeps query_string from
        // splitting at the space and dropping the field binding on the tail.
        if (isKeywordLike(indexType) && hasUnescapedWildcard(str)) {
          return unquoted(str).replace(" ", "\\ ");
        }

        if (isTextLike(indexType)) {
          // Quoting requests phrase semantics. One exception: a whitespace-free value carrying an
          // unescaped wildcard is emitted unquoted, because quoting would let the analyzer discard
          // the wildcard (`foo*` would stop matching `foobar`). That is only safe without
          // whitespace — with a space, an unquoted value would be split into separate clauses and
          // the tail would lose its field binding, so those stay phrases.
          boolean wildcardTerm = hasUnescapedWildcard(str) && !str.contains(" ");
          return userQuoted && !wildcardTerm ? quoted(str) : unquoted(str);
        }

        // Everything else — keyword without a wildcard, plus date, numeric, ip, boolean and
        // unresolved fields: legacy behavior, byte-identical to before this change.
        return str.contains(" ") ? quoted(str) : unquoted(str);
      }
    }

    return unquoted(literal.toString());
  }

  private static String quoted(String str) {
    return "\"" + QueryStringUtils.escapeLuceneSpecialCharacters(str) + "\"";
  }

  private static String unquoted(String str) {
    return QueryStringUtils.escapeLuceneSpecialCharacters(str);
  }

  private static boolean isTextLike(ExprType type) {
    if (type == null) {
      return false;
    }
    String legacyName = type.getOriginalExprType().legacyTypeName();
    return "TEXT".equalsIgnoreCase(legacyName) || "MATCH_ONLY_TEXT".equalsIgnoreCase(legacyName);
  }

  private static boolean isKeywordLike(ExprType type) {
    if (type == null) {
      return false;
    }
    String legacyName = type.getOriginalExprType().legacyTypeName();
    return "KEYWORD".equalsIgnoreCase(legacyName)
        || "CONSTANT_KEYWORD".equalsIgnoreCase(legacyName);
  }

  private static boolean hasUnescapedWildcard(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        i++;
        continue;
      }
      if (c == '*' || c == '?') {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toAnonymizedString() {
    return "***";
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Collections.singletonList(literal);
  }
}
