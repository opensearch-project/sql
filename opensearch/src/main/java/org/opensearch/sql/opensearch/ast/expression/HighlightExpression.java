/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.expression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.analysis.OpenSearchExpressionNodeVisitor;

/** Highlight Expression. */
@Getter
public class HighlightExpression extends OpenSearchFunctionExpression {
  private final Expression highlightField;
  private final ExprType type;

  /**
   * HighlightExpression Constructor.
   *
   * @param highlightField : Highlight field for expression.
   */
  public HighlightExpression(Expression highlightField) {
    super(BuiltinFunctionName.HIGHLIGHT.getName(), List.of(highlightField));
    this.highlightField = highlightField;
    this.type =
        this.highlightField.toString().contains("*") ? ExprCoreType.STRUCT : ExprCoreType.ARRAY;
  }

  /**
   * Return collection value matching highlight field.
   *
   * @param valueEnv : Dataset to parse value from.
   * @return : collection value of highlight fields.
   */
  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    String refName = "_highlight";
    // Not a wildcard expression
    if (this.type == ExprCoreType.ARRAY) {
      refName += "." + StringUtils.unquoteText(getHighlightField().toString());
    }
    ExprValue value = valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));

    // In the event of multiple returned highlights and wildcard being
    // used in conjunction with other highlight calls, we need to ensure
    // only wildcard regex matching is mapped to wildcard call.
    if (this.type == ExprCoreType.STRUCT && value.type() == ExprCoreType.STRUCT) {
      value =
          new ExprTupleValue(
              new LinkedHashMap<>(
                  value.tupleValue().entrySet().stream()
                      .filter(
                          s ->
                              matchesHighlightRegex(
                                  s.getKey(), StringUtils.unquoteText(highlightField.toString())))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
      if (value.tupleValue().isEmpty()) {
        value = ExprValueUtils.missingValue();
      }
    }

    return value;
  }

  /**
   * Get type for HighlightExpression.
   *
   * @return : Expression type.
   */
  @Override
  public ExprType type() {
    return this.type;
  }

  @Override
  public <T, C> T accept(OpenSearchExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }

  /**
   * Check if field matches the wildcard pattern used in highlight query.
   *
   * @param field Highlight selected field for query
   * @param pattern Wildcard regex to match field against
   * @return True if field matches wildcard pattern
   */
  private boolean matchesHighlightRegex(String field, String pattern) {
    Pattern p = Pattern.compile(pattern.replace("*", ".*"));
    Matcher matcher = p.matcher(field);
    return matcher.matches();
  }
}
