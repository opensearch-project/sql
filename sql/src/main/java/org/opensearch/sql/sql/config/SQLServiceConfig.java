/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.config;

import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * SQL service configuration for Spring container initialization.
 */
@Configuration
public class SQLServiceConfig {

  @Autowired
  private QueryManager queryManager;

  @Autowired
  private QueryPlanFactory queryExecutionFactory;

  /**
   * The registration of OpenSearch storage engine happens here because
   * OpenSearchStorageEngine is dependent on NodeClient.
   *
   * @return SQLService.
   */
  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public SQLService sqlService() {
    return new SQLService(
        new SQLSyntaxParser(),
        queryManager,
        queryExecutionFactory);
  }

}

