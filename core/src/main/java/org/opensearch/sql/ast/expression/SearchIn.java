/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/** Search expression for IN operator. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchIn extends SearchExpression {

  private final Field field;
  private final List<UnresolvedExpression> values;

  @Override
  public String toQueryString() {
    String fieldName = QueryStringUtils.escapeFieldName(field.getField().toString());
    String valueList = values.stream().map(this::formatValue).collect(Collectors.joining(" OR "));

    return fieldName + ":( " + valueList + " )";
  }

  private String formatValue(UnresolvedExpression expr) {
    if (expr instanceof Literal) {
      Literal lit = (Literal) expr;
      Object val = lit.getValue();

      // Numbers don't need escaping
      if (val instanceof Number) {
        return val.toString();
      }

      // Strings need Lucene escaping
      if (val instanceof String) {
        String str = (String) val;
        str = QueryStringUtils.escapeLuceneSpecialCharacters(str);
        // Add quotes if contains spaces
        if (str.contains(" ")) {
          return "\"" + str + "\"";
        }
        return str;
      }
    }

    return QueryStringUtils.escapeLuceneSpecialCharacters(expr.toString());
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    List<UnresolvedExpression> children = new ArrayList<>();
    children.add(field);
    children.addAll(values);
    return children;
  }
}
