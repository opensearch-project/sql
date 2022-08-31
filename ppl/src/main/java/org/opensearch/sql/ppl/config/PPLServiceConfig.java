/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.config;

import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.s3.storage.S3StorageEngine;
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
  private QueryService queryService;

  @Bean
  public Analyzer analyzer() {
    return new Analyzer(new ExpressionAnalyzer(functionRepository), catalogService());
  }

  @Bean
  public PPLService pplService() {
    return new PPLService(new PPLSyntaxParser(), analyzer(), storageEngine, executionEngine,
        functionRepository, queryService);
  }

  /**
   * Todo.
   */
  @Bean
  public CatalogService catalogService() {
    return new CatalogService(tableName -> {
      if (tableName.startsWith("s3")) {
        return new S3StorageEngine(queryService);
      } else {
        return storageEngine;
      }
    });
  }
}
