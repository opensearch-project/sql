/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * The eval operator evaluate the {@link EvalOperator#expressionList} and put the result into to
 * output. If the field name doesn't exist in the input, a new field will be append to the output.
 * If the field name exist in the input, a new value will be put into to output.
 *
 * <p>The {@link EvalOperator#expressionList} are evaluated from left to right. It means you can
 * reference previous evaluated field. e.g. fields velocity = distance/time, doubleVelocity = 2 *
 * velocity
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class EvalOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final List<Pair<ReferenceExpression, Expression>> expressionList;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitEval(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    Map<String, ExprValue> evalMap = eval(inputValue.bindingTuples());

    if (STRUCT == inputValue.type()) {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new Builder<>();
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      for (Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
        if (evalMap.containsKey(valueEntry.getKey())) {
          resultBuilder.put(valueEntry.getKey(), evalMap.get(valueEntry.getKey()));
          evalMap.remove(valueEntry.getKey());
        } else {
          resultBuilder.put(valueEntry);
        }
      }
      resultBuilder.putAll(evalMap);
      return ExprTupleValue.fromExprValueMap(resultBuilder.build());
    } else {
      return inputValue;
    }
  }

  /**
   * Evaluate the expression in the {@link EvalOperator#expressionList} with {@link Environment}.
   *
   * @param env {@link Environment}
   * @return The mapping of reference and {@link ExprValue} for each expression.
   */
  private Map<String, ExprValue> eval(Environment<Expression, ExprValue> env) {
    Map<String, ExprValue> evalResultMap = new LinkedHashMap<>();
    for (Pair<ReferenceExpression, Expression> pair : expressionList) {
      ReferenceExpression var = pair.getKey();
      ExprValue value = pair.getValue().valueOf(env);
      env = extendEnv(env, var, value);
      evalResultMap.put(var.toString(), value);
    }
    return evalResultMap;
  }
}
