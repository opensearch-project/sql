/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ParseExpression;
import org.opensearch.sql.utils.ParseUtils;

/**
 * Project the fields specified in {@link ProjectOperator#projectList} from input.
 */
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class ProjectOperator extends PhysicalPlan {
  private static final Logger log = LogManager.getLogger(ProjectOperator.class);
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final List<NamedExpression> projectList;
  @Getter
  private final Map<String, ParseExpression> parseExpressionMap;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    ImmutableMap.Builder<String, ExprValue> mapBuilder = new Builder<>();
    for (NamedExpression expr : projectList) {
      ExprValue exprValue = expr.valueOf(inputValue.bindingTuples());
      if (!parseExpressionMap.containsKey(expr.getNameOrAlias())) {
        mapBuilder.put(expr.getNameOrAlias(), exprValue);
      }
    }
    // ParseExpression will always override NamedExpression when identifier conflicts
    for (ParseExpression expr : parseExpressionMap.values()) {
      ExprValue value = inputValue.bindingTuples().resolve(expr.getExpression());
      if (value.isMissing()) {
        // value will be missing after stats command, read from inputValue if it exists
        // otherwise do nothing since it should not appear as a field
        ExprValue exprValue = ExprValueUtils.getTupleValue(inputValue).get(expr.getIdentifier());
        if (exprValue != null) {
          mapBuilder.put(expr.getIdentifier(), exprValue);
        }
      } else {
        ExprValue parsedValue =
            ParseUtils.parseValue(value, expr.getPattern(), expr.getIdentifier());
        mapBuilder.put(expr.getIdentifier(), parsedValue);
      }
    }
    return ExprTupleValue.fromExprValueMap(mapBuilder.build());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(getProjectList().stream()
        .map(expr -> new ExecutionEngine.Schema.Column(expr.getName(),
            expr.getAlias(), expr.type())).collect(Collectors.toList()));
  }
}
