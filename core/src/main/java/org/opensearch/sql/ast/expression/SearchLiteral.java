/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Search expression for standalone literals. */
@Getter
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchLiteral extends SearchExpression {

  private final UnresolvedExpression literal;
  private final boolean isPhrase;
  private final String rawText; // Optional raw text to preserve

  public SearchLiteral(UnresolvedExpression literal) {
    this(literal, false, null);
  }

  public SearchLiteral(UnresolvedExpression literal, boolean isPhrase) {
    this(literal, isPhrase, null);
  }

  public SearchLiteral(String rawText) {
    this(null, false, rawText);
  }

  @Override
  public String toQueryString() {
    // If raw text is provided, use it directly (for preserving numeric suffixes)
    if (rawText != null) {
      return rawText;
    }

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

        // Phrase search - preserve quotes
        if (isPhrase) {
          // Escape special chars inside the phrase
          str = QueryStringUtils.escapeLuceneSpecialCharacters(str);
          return "\"" + str + "\"";
        }

        // Regular string - escape special characters
        return QueryStringUtils.escapeLuceneSpecialCharacters(str);
      }
    }

    // Default: escape the text representation
    String text = literal.toString();
    return QueryStringUtils.escapeLuceneSpecialCharacters(text);
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Collections.singletonList(literal);
  }
}
