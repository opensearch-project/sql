/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.opensearch.sql.datasource.model.DataSourceType;

public class GrammarElementValidatorFactory {

  private static GrammarElementValidator defaultValidator =
      new DenyListGrammarElementValidator(ImmutableSet.of());
  private static Map<DataSourceType, GrammarElementValidator> validatorMap =
      ImmutableMap.of(
          DataSourceType.S3GLUE, new S3GlueGrammarElementValidator(),
          DataSourceType.SECURITY_LAKE, new SecurityLakeGrammarElementValidator());

  public GrammarElementValidator getValidatorForDatasource(DataSourceType dataSourceType) {
    return validatorMap.getOrDefault(dataSourceType, defaultValidator);
  }
}
