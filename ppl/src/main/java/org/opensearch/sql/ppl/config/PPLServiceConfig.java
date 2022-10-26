/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.config;

import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class PPLServiceConfig {

  @Autowired
  private QueryManager queryManager;

  @Autowired
  private QueryPlanFactory queryPlanFactory;

  /**
   * The registration of OpenSearch storage engine happens here because
   * OpenSearchStorageEngine is dependent on NodeClient.
   *
   * @return PPLService.
   */
  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public PPLService pplService() {
    return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory);
  }
}
