/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.utils.QueryStringUtils;

/** Search expression for IN operator. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchIn extends SearchExpression {

  private final Field field;
  private final List<SearchLiteral> values;

  @Override
  public String toQueryString(Function<String, ExprType> fieldTypeResolver) {
    String rawFieldName = field.getField().toString();
    String fieldName = QueryStringUtils.escapeFieldName(field.getField().toString());
    ExprType resolvedType = fieldTypeResolver.apply(rawFieldName);
    String valueList =
        values.stream().map(v -> v.toQueryString(resolvedType)).collect(Collectors.joining(" OR "));

    return fieldName + ":( " + valueList + " )";
  }

  @Override
  public String toAnonymizedString() {
    return "identifier IN ***";
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    List<UnresolvedExpression> children = new ArrayList<>();
    children.add(field);
    // SearchLiteral extends SearchExpression which extends UnresolvedExpression
    children.addAll(values);
    return children;
  }
}
