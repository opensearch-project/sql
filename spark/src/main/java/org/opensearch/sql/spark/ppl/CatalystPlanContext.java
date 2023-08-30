/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.ppl;

import lombok.Getter;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.analysis.TypeEnvironment;
import org.opensearch.sql.expression.function.FunctionProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The context used for Catalyst logical plan. */
public class CatalystPlanContext {
  /** Environment stack for symbol scope management. */
  private TypeEnvironment environment;

  @Getter private LogicalPlan plan;
  
  @Getter private final List<NamedExpression> namedParseExpressions;

  @Getter private final FunctionProperties functionProperties;

  public CatalystPlanContext() {
    this(new TypeEnvironment(null));
  }

  /**
   * Class CTOR.
   *
   * @param environment Env to set to a new instance.
   */
  public CatalystPlanContext(TypeEnvironment environment) {
    this.environment = environment;
    this.namedParseExpressions = new ArrayList<>();
    this.functionProperties = new FunctionProperties();
  }

  /** Push a new environment. */
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
   * update context with evolving plan
   * @param plan
   */
  public void plan(LogicalPlan plan) {
    this.plan = plan;
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
}
