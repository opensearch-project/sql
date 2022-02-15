/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ParseExpression;
import org.opensearch.sql.planner.logical.LogicalParse;

/**
 * The context used for Analyzer.
 */
public class AnalysisContext {
  /**
   * Environment stack for symbol scope management.
   */
  private TypeEnvironment environment;

  private Map<String, ParseExpression> parseExpressionMap;

  public LogicalParse parse;

  public AnalysisContext() {
    this.environment = new TypeEnvironment(null);
    this.parseExpressionMap = new HashMap<>();
  }

  public AnalysisContext(TypeEnvironment environment) {
    this.environment = environment;
  }

  /**
   * Push a new environment.
   */
  public void push() {
    environment = new TypeEnvironment(environment);
  }

  /**
   * Return current environment.
   *
   * @return current environment
   */
  public TypeEnvironment peek() {
    return environment;
  }

  /**
   * Pop up current environment from environment chain.
   *
   * @return current environment (before pop)
   */
  public TypeEnvironment pop() {
    Objects.requireNonNull(environment, "Fail to pop context due to no environment present");

    TypeEnvironment curEnv = environment;
    environment = curEnv.getParent();
    return curEnv;
  }

  public void addParseExpression(String identifier, ParseExpression expression) {
    parseExpressionMap.put(identifier, expression);
  }

  public ParseExpression getParseExpression(String identifier) {
    return parseExpressionMap.get(identifier);
  }

  public boolean isParseExpression(String identifier) {
    return parseExpressionMap.containsKey(identifier);
  }

  public List<ParseExpression> getParseExpressionList() {
    return new ArrayList<>(parseExpressionMap.values());
  }
}
