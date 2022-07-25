package org.opensearch.sql.analysis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.common.utils.StringUtils;

@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class HighlightExpression extends FunctionExpression {
  Expression highlightField;

  public HighlightExpression(Expression highlightField) {
    super(BuiltinFunctionName.HIGHLIGHT.getName(), List.of(highlightField));
    this.highlightField = highlightField;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    String refName = "_highlight";
    if(!getHighlightField().toString().contains("*")) {
      refName += "." + StringUtils.unquoteText(getHighlightField().toString());
    }
    ExprValue temp = valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));
    ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();

    if(temp.isMissing() || temp.type() == ExprCoreType.STRING) {
      return temp;
    } else if(temp.tupleValue().entrySet().size() > 1) {
      var hlBuilder = ImmutableMap.<String, ExprValue>builder();
      hlBuilder.putAll(temp.tupleValue());

      for (var foo : temp.tupleValue().entrySet()) {
        String blah = "highlight_" + foo.getKey();
        builder.put(blah, ExprValueUtils.stringValue(foo.getValue().toString()));
      }

      return ExprTupleValue.fromExprValueMap(builder.build());
    }
    return temp.tupleValue().get(StringUtils.unquoteText(this.highlightField.toString()));
//    return ExprTupleValue.fromExprValueMap(builder.build());
//    String refName = "_highlight(" + highlightField.toString() + ")";
//    return valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  //  @Override
  //  public <T, C> T accept(org.opensearch.sql.expression.ExpressionNodeVisitor<T, C> visitor,
  //                         C context) {
  //    return visitor.visitHighlight(this, context);
  //  }
}
