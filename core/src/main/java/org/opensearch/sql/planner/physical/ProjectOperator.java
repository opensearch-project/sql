/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ParseExpression;

/**
 * Project the fields specified in {@link ProjectOperator#projectList} from input.
 */
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class ProjectOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final List<NamedExpression> projectList;
  @Getter
  private final List<NamedExpression> namedParseExpressions;

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
      if (namedParseExpressions.stream()
          .noneMatch(parsed -> parsed.getNameOrAlias().equals(expr.getNameOrAlias()))) {
        mapBuilder.put(expr.getNameOrAlias(), exprValue);
      }
    }
    // ParseExpression will always override NamedExpression when identifier conflicts
    // TODO needs a better implementation, see https://github.com/opensearch-project/sql/issues/458
    for (NamedExpression expr : namedParseExpressions) {
      ExprValue value = inputValue.bindingTuples()
          .resolve(((ParseExpression) expr.getDelegated()).getExpression());
      if (value.isMissing()) {
        // value will be missing after stats command, read from inputValue if it exists
        // otherwise do nothing since it should not appear as a field
        ExprValue exprValue = ExprValueUtils.getTupleValue(inputValue).get(expr.getNameOrAlias());
        if (exprValue != null) {
          mapBuilder.put(expr.getNameOrAlias(), exprValue);
        }
      } else {
        ExprValue parsedValue = expr.valueOf(inputValue.bindingTuples());
        mapBuilder.put(expr.getNameOrAlias(), parsedValue);
      }
    }
    return ExprTupleValue.fromExprValueMap(mapBuilder.build());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(getProjectList().stream()
        .map(expr -> new ExecutionEngine.Schema.Column(
                StringUtils.unescapeBackslashes(expr.getName()),
                expr.getAlias() == null ? null : StringUtils.unescapeBackslashes(expr.getAlias()),
                expr.type())
        ).collect(Collectors.toList()));
  }
}
