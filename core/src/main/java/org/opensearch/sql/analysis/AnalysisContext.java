/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import java.util.Objects;

/**
 * The context used for Analyzer.
 */
public class AnalysisContext {
  /**
   * Environment stack for symbol scope management.
   */
  private TypeEnvironment environment;

  public AnalysisContext() {
    this.environment = new TypeEnvironment(null);
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
}
