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
  final S3GlueSQLGrammarElementValidator s3GlueSQLGrammarElementValidator =
      new S3GlueSQLGrammarElementValidator();
  final SecurityLakeSQLGrammarElementValidator securityLakeSQLGrammarElementValidator =
      new SecurityLakeSQLGrammarElementValidator();
  final DefaultGrammarElementValidator defaultGrammarElementValidator =
      new DefaultGrammarElementValidator();
  final GrammarElementValidatorProvider grammarElementValidatorProvider =
      new GrammarElementValidatorProvider(
          ImmutableMap.of(
              DataSourceType.S3GLUE, s3GlueSQLGrammarElementValidator,
              DataSourceType.SECURITY_LAKE, securityLakeSQLGrammarElementValidator),
          defaultGrammarElementValidator);

  @Test
  public void test() {
    assertEquals(
        s3GlueSQLGrammarElementValidator,
        grammarElementValidatorProvider.getValidatorForDatasource(DataSourceType.S3GLUE));
    assertEquals(
        securityLakeSQLGrammarElementValidator,
        grammarElementValidatorProvider.getValidatorForDatasource(DataSourceType.SECURITY_LAKE));
    assertEquals(
        defaultGrammarElementValidator,
        grammarElementValidatorProvider.getValidatorForDatasource(DataSourceType.PROMETHEUS));
  }
}
