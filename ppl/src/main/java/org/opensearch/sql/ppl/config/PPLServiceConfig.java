/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.config;

import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.storage.CatalogService;
import org.opensearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ExpressionConfig.class})
public class PPLServiceConfig {

  @Autowired
  private StorageEngine storageEngine;

  @Autowired
  private ExecutionEngine executionEngine;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  @Autowired
  private CatalogService catalogService;

  @Bean
  public Analyzer analyzer() {
    return new Analyzer(new ExpressionAnalyzer(functionRepository), catalogService);
  }

  @Bean
  public PPLService pplService() {
    return new PPLService(new PPLSyntaxParser(), analyzer(), storageEngine, executionEngine,
        functionRepository);
  }

}
