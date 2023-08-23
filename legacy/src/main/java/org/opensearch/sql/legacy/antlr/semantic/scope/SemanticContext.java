/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

import java.util.Objects;

/**
 * Semantic context responsible for environment chain (stack) management and everything required for
 * analysis. This context should be shared by different stages in future, particularly from semantic
 * analysis to logical planning to physical planning.
 */
public class SemanticContext {

  /** Environment stack for symbol scope management */
  private Environment environment = new Environment(null);

  /** Push a new environment */
  public void push() {
    environment = new Environment(environment);
  }

  /**
   * Return current environment
   *
   * @return current environment
   */
  public Environment peek() {
    return environment;
  }

  /**
   * Pop up current environment from environment chain
   *
   * @return current environment (before pop)
   */
  public Environment pop() {
    Objects.requireNonNull(environment, "Fail to pop context due to no environment present");

    Environment curEnv = environment;
    environment = curEnv.getParent();
    return curEnv;
  }
}
