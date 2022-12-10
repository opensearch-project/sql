/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FunctionPropertiesTestConfig {
  @Bean
  FunctionProperties functionProperties() {
    return new FunctionProperties();
  }
}
