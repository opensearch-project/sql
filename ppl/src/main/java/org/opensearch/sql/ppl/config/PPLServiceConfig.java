/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.ppl.config;

import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
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

  @Bean
  public Analyzer analyzer() {
    return new Analyzer(new ExpressionAnalyzer(functionRepository), storageEngine);
  }

  @Bean
  public PPLService pplService() {
    return new PPLService(new PPLSyntaxParser(), analyzer(), storageEngine, executionEngine,
        functionRepository);
  }

}
