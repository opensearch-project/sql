/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.expression.env.Environment.extendEnv;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * OpenSearch version of eval operator, which contains nodeClient, in order to perform OpenSearch
 * related operation during the eval process.
 */
public class OpenSearchEvalOperator extends EvalOperator {

  @Getter private final NodeClient nodeClient;

  public OpenSearchEvalOperator(
      PhysicalPlan input,
      List<Pair<ReferenceExpression, Expression>> expressionList,
      NodeClient nodeClient) {
    super(input, expressionList);
    this.nodeClient = nodeClient;
  }

  /**
   * Evaluate the expression in the {@link EvalOperator} with {@link Environment}.
   *
   * @param env {@link Environment}
   * @return The mapping of reference and {@link ExprValue} for each expression.
   */
  @Override
  protected Map<String, ExprValue> eval(Environment<Expression, ExprValue> env) {
    Map<String, ExprValue> evalResultMap = new LinkedHashMap<>();
    for (Pair<ReferenceExpression, Expression> pair : this.getExpressionList()) {
      ExprValue value;
      if (pair.getValue() instanceof OpenSearchFunctions.OpenSearchFunction openSearchExpr) {
        value = OpenSearchEvalProcessor.process(openSearchExpr, env, nodeClient);
      } else {
        value = pair.getValue().valueOf(env);
      }
      ReferenceExpression var = pair.getKey();
      env = extendEnv(env, var, value);
      evalResultMap.put(var.toString(), value);
    }
    return evalResultMap;
  }
}
