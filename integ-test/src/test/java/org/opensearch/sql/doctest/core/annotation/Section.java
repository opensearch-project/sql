/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.annotation;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used to indicate current method is a valid method for doc generation
 * and it is supposed to run in the specified order.
 */
@Retention(RUNTIME)
@Target(value = METHOD)
public @interface Section {

  /**
   * @return section order
   */
  int value() default 0;

}
