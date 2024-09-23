/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.opensearch.sql.spark.validator.GrammarElement.ALTER_NAMESPACE;
import static org.opensearch.sql.spark.validator.GrammarElement.ALTER_VIEW;
import static org.opensearch.sql.spark.validator.GrammarElement.ANALYZE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.CACHE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.CLEAR_CACHE;
import static org.opensearch.sql.spark.validator.GrammarElement.CLUSTER_BY;
import static org.opensearch.sql.spark.validator.GrammarElement.CREATE_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.CREATE_NAMESPACE;
import static org.opensearch.sql.spark.validator.GrammarElement.CREATE_VIEW;
import static org.opensearch.sql.spark.validator.GrammarElement.CSV_FUNCTIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.DESCRIBE_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.DESCRIBE_NAMESPACE;
import static org.opensearch.sql.spark.validator.GrammarElement.DESCRIBE_QUERY;
import static org.opensearch.sql.spark.validator.GrammarElement.DESCRIBE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.DISTRIBUTE_BY;
import static org.opensearch.sql.spark.validator.GrammarElement.DROP_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.DROP_NAMESPACE;
import static org.opensearch.sql.spark.validator.GrammarElement.DROP_VIEW;
import static org.opensearch.sql.spark.validator.GrammarElement.FILE;
import static org.opensearch.sql.spark.validator.GrammarElement.HINTS;
import static org.opensearch.sql.spark.validator.GrammarElement.INLINE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.INSERT;
import static org.opensearch.sql.spark.validator.GrammarElement.LOAD;
import static org.opensearch.sql.spark.validator.GrammarElement.MANAGE_RESOURCE;
import static org.opensearch.sql.spark.validator.GrammarElement.MISC_FUNCTIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.REFRESH_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.REFRESH_RESOURCE;
import static org.opensearch.sql.spark.validator.GrammarElement.REFRESH_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.REPAIR_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.RESET;
import static org.opensearch.sql.spark.validator.GrammarElement.SET;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_COLUMNS;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_CREATE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_FUNCTIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_NAMESPACES;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_PARTITIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_TABLES;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_TABLE_EXTENDED;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_TBLPROPERTIES;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_VIEWS;
import static org.opensearch.sql.spark.validator.GrammarElement.TABLESAMPLE;
import static org.opensearch.sql.spark.validator.GrammarElement.TABLE_VALUED_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.TRANSFORM;
import static org.opensearch.sql.spark.validator.GrammarElement.TRUNCATE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.UDF;
import static org.opensearch.sql.spark.validator.GrammarElement.UNCACHE_TABLE;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class SecurityLakeGrammarElementValidator extends DenyListGrammarElementValidator {
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

  public SecurityLakeGrammarElementValidator() {
    super(SECURITY_LAKE_DENY_LIST);
  }
}
