/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.opensearch.sql.spark.validator.GrammarElement.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.datasource.model.DataSourceType;

public class GrammarElementValidatorFactory {
  private static final Set<GrammarElement> DEFAULT_DENY_LIST =
      ImmutableSet.of(CREATE_FUNCTION, DROP_FUNCTION, INSERT, LOAD, HINTS, TABLESAMPLE);

  private static final Set<GrammarElement> CWL_DENY_LIST =
      copyBuilder(DEFAULT_DENY_LIST)
          .add(
              ALTER_NAMESPACE,
              ALTER_VIEW,
              CREATE_NAMESPACE,
              CREATE_VIEW,
              DROP_NAMESPACE,
              DROP_VIEW,
              REPAIR_TABLE,
              TRUNCATE_TABLE)
          .build();

  private static final Set<GrammarElement> S3GLUE_DENY_LIST =
      copyBuilder(DEFAULT_DENY_LIST)
          .add(
              ALTER_VIEW,
              CREATE_VIEW,
              DROP_VIEW,
              REPAIR_TABLE,
              DISTRIBUTE_BY,
              INLINE_TABLE,
              TRUNCATE_TABLE,
              CLUSTER_BY,
              DISTRIBUTE_BY,
              CROSS_JOIN,
              LEFT_SEMI_JOIN,
              RIGHT_OUTER_JOIN,
              FULL_OUTER_JOIN,
              LEFT_ANTI_JOIN,
              TABLESAMPLE,
              TABLE_VALUED_FUNCTION,
              TRANSFORM,
              MANAGE_RESOURCE,
              DESCRIBE_FUNCTION,
              REFRESH_RESOURCE,
              REFRESH_FUNCTION,
              RESET,
              SET,
              SHOW_FUNCTIONS,
              SHOW_VIEWS,
              MISC_FUNCTIONS)
          .build();

  private static Map<DataSourceType, GrammarElementValidator> validatorMap =
      ImmutableMap.of(DataSourceType.S3GLUE, new DenyListGrammarElementValidator(S3GLUE_DENY_LIST));

  public GrammarElementValidator getValidatorForDatasource(DataSourceType dataSourceType) {
    return validatorMap.get(dataSourceType);
  }

  private static ImmutableSet.Builder<GrammarElement> copyBuilder(Set<GrammarElement> original) {
    return ImmutableSet.<GrammarElement>builder().addAll(original);
  }
}
