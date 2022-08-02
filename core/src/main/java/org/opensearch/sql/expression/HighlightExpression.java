/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import lombok.Getter;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * Highlight Expression.
 */
@Getter
public class HighlightExpression extends FunctionExpression {
  private final Expression highlightField;

  /**
   * HighlightExpression Constructor.
   * @param highlightField : Highlight field for expression.
   */
  public HighlightExpression(Expression highlightField) {
    super(BuiltinFunctionName.HIGHLIGHT.getName(), List.of(highlightField));
    this.highlightField = highlightField;
  }

  /**
   * Return String or Map value matching highlight field.
   * @param valueEnv : Dataset to parse value from.
   * @return : String or Map value of highlight fields.
   */
  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    String refName = "_highlight";
    if (!getHighlightField().toString().contains("*")) {
      refName += "." + StringUtils.unquoteText(getHighlightField().toString());
    }
    ExprValue retVal = valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));

    // If only one highlight returned, or no highlights can be parsed.
    if (retVal.isMissing() || retVal.type() != ExprCoreType.STRUCT) {
      return retVal;
    }

    var highlightMapBuilder = ImmutableMap.<String, ExprValue>builder();
    highlightMapBuilder.putAll(retVal.tupleValue());
    ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
    for (var entry : retVal.tupleValue().entrySet()) {
      String entryKey = "highlight(" + getHighlightField() + ")." + entry.getKey();
      builder.put(entryKey, ExprValueUtils.stringValue(entry.getValue().toString()));
    }

    return ExprTupleValue.fromExprValueMap(builder.build());
  }

  /**
   * Get type for HighlightExpression.
   * @return : String type.
   */
  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }
}
