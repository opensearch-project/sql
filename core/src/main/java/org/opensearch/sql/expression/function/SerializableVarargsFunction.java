/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import java.io.Serializable;

/**
 * Serializable Varargs Function.
 */
public interface SerializableVarargsFunction<T, R> extends Serializable {
  /**
   * Applies this function to the given arguments.
   *
   * @param t the function argument
   * @return the function result
   */
  R apply(T... t);
}
