/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.model.DataSourceType;

class GrammarElementValidatorProviderTest {
  S3GlueGrammarElementValidator s3GlueGrammarElementValidator = new S3GlueGrammarElementValidator();
  SecurityLakeGrammarElementValidator securityLakeGrammarElementValidator =
      new SecurityLakeGrammarElementValidator();
  DefaultGrammarElementValidator defaultGrammarElementValidator =
      new DefaultGrammarElementValidator();
  GrammarElementValidatorProvider grammarElementValidatorProvider =
      new GrammarElementValidatorProvider(
          ImmutableMap.of(
              DataSourceType.S3GLUE, s3GlueGrammarElementValidator,
              DataSourceType.SECURITY_LAKE, securityLakeGrammarElementValidator),
          defaultGrammarElementValidator);

  @Test
  public void test() {
    assertEquals(
        s3GlueGrammarElementValidator,
        grammarElementValidatorProvider.getValidatorForDatasource(DataSourceType.S3GLUE));
    assertEquals(
        securityLakeGrammarElementValidator,
        grammarElementValidatorProvider.getValidatorForDatasource(DataSourceType.SECURITY_LAKE));
    assertEquals(
        defaultGrammarElementValidator,
        grammarElementValidatorProvider.getValidatorForDatasource(DataSourceType.PROMETHEUS));
  }
}
