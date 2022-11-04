/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.config;

import org.opensearch.sql.datasource.DatasourceService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ExpressionConfig.class})
public class PPLServiceConfig {

  @Autowired
  private ExecutionEngine executionEngine;

  @Autowired
  private DatasourceService datasourceService;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  /**
   * The registration of OpenSearch storage engine happens here because
   * OpenSearchStorageEngine is dependent on NodeClient.
   *
   * @return PPLService.
   */
  @Bean
  public PPLService pplService() {
    datasourceService.getDatasources()
        .forEach(datasource -> datasource.getStorageEngine().getFunctions()
            .forEach(functionResolver -> functionRepository
                .register(datasource.getName(), functionResolver)));
    return new PPLService(new PPLSyntaxParser(), executionEngine,
            functionRepository, datasourceService);
  }

}
