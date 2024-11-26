/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.opensearch.sql.spark.validator.SQLGrammarElement.ALTER_NAMESPACE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.ALTER_VIEW;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.ANALYZE_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CACHE_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CLEAR_CACHE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CLUSTER_BY;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CREATE_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CREATE_NAMESPACE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CREATE_VIEW;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CROSS_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CSV_FUNCTIONS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DESCRIBE_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DESCRIBE_NAMESPACE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DESCRIBE_QUERY;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DESCRIBE_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DISTRIBUTE_BY;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DROP_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DROP_NAMESPACE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DROP_VIEW;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.FILE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.FULL_OUTER_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.HINTS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.INLINE_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.INSERT;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.LEFT_ANTI_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.LEFT_SEMI_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.LOAD;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.MANAGE_RESOURCE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.MISC_FUNCTIONS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.REFRESH_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.REFRESH_RESOURCE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.REFRESH_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.REPAIR_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.RESET;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.RIGHT_OUTER_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SET;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_COLUMNS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_CREATE_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_FUNCTIONS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_NAMESPACES;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_PARTITIONS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_TABLES;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_TABLE_EXTENDED;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_TBLPROPERTIES;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_VIEWS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TABLESAMPLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TABLE_VALUED_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TRANSFORM;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TRUNCATE_TABLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.UDF;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.UNCACHE_TABLE;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class SecurityLakeSQLGrammarElementValidator extends DenyListGrammarElementValidator {
  private static final Set<GrammarElement> SECURITY_LAKE_DENY_LIST =
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

  public SecurityLakeSQLGrammarElementValidator() {
    super(SECURITY_LAKE_DENY_LIST);
  }
}
