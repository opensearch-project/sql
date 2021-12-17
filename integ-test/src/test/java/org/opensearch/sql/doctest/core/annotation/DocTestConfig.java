/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Configuration to initializing the set up for a doc test.
 */
@Retention(RUNTIME)
@Target(value = TYPE)
public @interface DocTestConfig {

  /**
   * Path of the template
   *
   * @return path
   */
  String template();

  /**
   * Path of the test data used.
   *
   * @return path
   */
  String[] testData() default {};

}
