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
  private final boolean isPhrase;

  @Override
  public String toQueryString(Function<String, ExprType> fieldTypeResolver) {
    // Unfielded literal: no enclosing field, so no index type. Take the field-agnostic branch.
    return toQueryString((ExprType) null);
  }

  /**
   * Emits the query_string form for a literal on the RHS of {@link SearchComparison} or inside
   * {@link SearchIn}. The decision tree is documented in {@code
   * docs/dev/ppl-search-command-contract-empirical.md} — briefly: whitespace + wildcard on a
   * non-text index escapes the space so the parser keeps the value as one whole-value pattern;
   * everything else falls through to phrase (with whitespace) or unquoted-escaped (without).
   *
   * @param indexType the enclosing field's OpenSearch index-mapping type (text/keyword/...) — used
   *     only to distinguish text-like from everything else; null means unknown, treated as
   *     text-like so we don't regress the phrase form.
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

        // [D] whitespace + wildcard on a non-text index: single term with space escaped, so the
        // query_string parser keeps the value as one whole-value pattern (a raw space would
        // split it into two clauses and drop the field binding on the right half).
        if (isPhrase && !isTextLike(indexType) && hasUnescapedWildcard(str)) {
          return QueryStringUtils.escapeLuceneSpecialCharacters(str).replace(" ", "\\ ");
        }

        // [B]/[C] quoted phrase.
        if (isPhrase) {
          str = QueryStringUtils.escapeLuceneSpecialCharacters(str);
          return "\"" + str + "\"";
        }

        // [A] unquoted; escape Lucene specials, wildcards preserved.
        return QueryStringUtils.escapeLuceneSpecialCharacters(str);
      }
    }

    String text = literal.toString();
    return QueryStringUtils.escapeLuceneSpecialCharacters(text);
  }

  private static boolean isTextLike(ExprType type) {
    if (type == null) {
      // Unknown type → treat as text-like so we take the phrase branch and avoid a text
      // regression when the resolver fails to identify the field.
      return true;
    }
    String legacyName = type.getOriginalExprType().legacyTypeName();
    return "TEXT".equalsIgnoreCase(legacyName) || "MATCH_ONLY_TEXT".equalsIgnoreCase(legacyName);
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
