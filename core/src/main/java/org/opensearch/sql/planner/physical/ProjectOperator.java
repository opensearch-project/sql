/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.opensearch.sql.exception.ExpressionEvaluationException;
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
  private static final Logger log = LogManager.getLogger(ProjectOperator.class);
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final List<NamedExpression> projectList;
  @Getter
  private final List<ParseExpression> parseExpressionList;

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
    Set<String> parsedFields =
        parseExpressionList.stream().map(parseExpression -> parseExpression.getIdentifier())
            .collect(
                Collectors.toSet());
    for (NamedExpression expr : projectList) {
      ExprValue exprValue = expr.valueOf(inputValue.bindingTuples());
      if (!parsedFields.contains(expr.getNameOrAlias())) {
        mapBuilder.put(expr.getNameOrAlias(), exprValue);
      }
    }
    for (ParseExpression expr : parseExpressionList) {
      ExprValue value = inputValue.bindingTuples().resolve(expr.getExpression());
      Pattern pattern = expr.getPattern();
      String identifier = expr.getIdentifier();
      try {
        String rawString = value.stringValue();
        Matcher matcher = pattern.matcher(rawString);
        if (matcher.matches()) {
          mapBuilder.put(identifier, new ExprStringValue(matcher.group(identifier)));
        } else {
          log.warn("failed to extract pattern {} from input {}", pattern.pattern(), rawString);
          mapBuilder.put(identifier, new ExprStringValue(""));
        }
      } catch (ExpressionEvaluationException e) {
        if (inputValue.tupleValue().containsKey(identifier)) {
          mapBuilder.put(identifier, inputValue.tupleValue().get(identifier));
        }
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
