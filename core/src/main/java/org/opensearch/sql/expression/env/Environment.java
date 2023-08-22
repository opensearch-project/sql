/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.env;

/**
 * The definition of the environment.
 *
 * @param <E> the type of expression
 * @param <V> the type of expression value
 */
public interface Environment<E, V> {

  /** resolve the value of expression from the environment. */
  V resolve(E var);

  /**
   * Extend the environment.
   *
   * @param env environment
   * @param expr expression.
   * @param value expression value.
   * @param <E> the type of expression
   * @param <V> the type of expression value
   * @return extended environment.
   */
  static <E, V> Environment<E, V> extendEnv(Environment<E, V> env, E expr, V value) {
    return var -> {
      if (var.equals(expr)) {
        return value;
      } else {
        return env.resolve(var);
      }
    };
  }
}
