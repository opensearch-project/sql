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
  // Deny List for CloudWatch Logs datasource
  private static final Set<GrammarElement> CWL_DENY_LIST =
      ImmutableSet.<GrammarElement>builder()
          .add(
              ALTER_NAMESPACE,
              ALTER_VIEW,
              CREATE_NAMESPACE,
              CREATE_FUNCTION,
              CREATE_VIEW,
              DROP_FUNCTION,
              DROP_NAMESPACE,
              DROP_VIEW,
              REPAIR_TABLE,
              TRUNCATE_TABLE,
              INSERT,
              LOAD,
              EXPLAIN,
              WITH,
              CLUSTER_BY,
              DISTRIBUTE_BY,
              HINTS,
              INLINE_TABLE,
              FILE,
              CROSS_JOIN,
              LEFT_SEMI_JOIN,
              RIGHT_OUTER_JOIN,
              FULL_OUTER_JOIN,
              LEFT_ANTI_JOIN,
              TABLESAMPLE,
              TABLE_VALUED_FUNCTION,
              LATERAL_VIEW,
              LATERAL_SUBQUERY,
              TRANSFORM,
              MANAGE_RESOURCE,
              ANALYZE_TABLE,
              CACHE_TABLE,
              DESCRIBE_NAMESPACE,
              DESCRIBE_FUNCTION,
              DESCRIBE_QUERY,
              DESCRIBE_TABLE,
              REFRESH_RESOURCE,
              REFRESH_TABLE,
              REFRESH_FUNCTION,
              RESET,
              SET,
              SHOW_COLUMNS,
              SHOW_CREATE_TABLE,
              SHOW_NAMESPACES,
              SHOW_FUNCTIONS,
              SHOW_PARTITIONS,
              SHOW_TABLE_EXTENDED,
              SHOW_TABLES,
              SHOW_TBLPROPERTIES,
              SHOW_VIEWS,
              UNCACHE_TABLE,
              CSV_FUNCTIONS,
              MISC_FUNCTIONS,
              UDF)
          .build();

  // Deny list for S3 Glue datasource
  private static final Set<GrammarElement> S3GLUE_DENY_LIST =
      ImmutableSet.<GrammarElement>builder()
          .add(
              ALTER_VIEW,
              CREATE_FUNCTION,
              CREATE_VIEW,
              DROP_FUNCTION,
              DROP_VIEW,
              INSERT,
              LOAD,
              CLUSTER_BY,
              DISTRIBUTE_BY,
              HINTS,
              INLINE_TABLE,
              FILE,
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
              MISC_FUNCTIONS,
              UDF)
          .build();

  // Deny list for Security Lake datasource
  private static final Set<GrammarElement> SL_DENY_LIST =
      ImmutableSet.<GrammarElement>builder()
          .add(
              ALTER_NAMESPACE,
              ALTER_VIEW,
              CREATE_NAMESPACE,
              CREATE_FUNCTION,
              CREATE_VIEW,
              DROP_FUNCTION,
              DROP_NAMESPACE,
              DROP_VIEW,
              REPAIR_TABLE,
              TRUNCATE_TABLE,
              INSERT,
              LOAD,
              CLUSTER_BY,
              DISTRIBUTE_BY,
              HINTS,
              INLINE_TABLE,
              FILE,
              CROSS_JOIN,
              LEFT_SEMI_JOIN,
              RIGHT_OUTER_JOIN,
              FULL_OUTER_JOIN,
              LEFT_ANTI_JOIN,
              TABLESAMPLE,
              TABLE_VALUED_FUNCTION,
              TRANSFORM,
              MANAGE_RESOURCE,
              ANALYZE_TABLE,
              CACHE_TABLE,
              CLEAR_CACHE,
              DESCRIBE_NAMESPACE,
              DESCRIBE_FUNCTION,
              DESCRIBE_QUERY,
              DESCRIBE_TABLE,
              REFRESH_RESOURCE,
              REFRESH_TABLE,
              REFRESH_FUNCTION,
              RESET,
              SET,
              SHOW_COLUMNS,
              SHOW_CREATE_TABLE,
              SHOW_NAMESPACES,
              SHOW_FUNCTIONS,
              SHOW_PARTITIONS,
              SHOW_TABLE_EXTENDED,
              SHOW_TABLES,
              SHOW_TBLPROPERTIES,
              SHOW_VIEWS,
              UNCACHE_TABLE,
              CSV_FUNCTIONS,
              MISC_FUNCTIONS,
              UDF)
          .build();

  private static Map<DataSourceType, GrammarElementValidator> validatorMap =
      ImmutableMap.of(
          DataSourceType.S3GLUE, new DenyListGrammarElementValidator(S3GLUE_DENY_LIST),
          DataSourceType.SECURITY_LAKE, new DenyListGrammarElementValidator(SL_DENY_LIST));

  public GrammarElementValidator getValidatorForDatasource(DataSourceType dataSourceType) {
    return validatorMap.get(dataSourceType);
  }

  private static ImmutableSet.Builder<GrammarElement> copyBuilder(Set<GrammarElement> original) {
    return ImmutableSet.<GrammarElement>builder().addAll(original);
  }
}
