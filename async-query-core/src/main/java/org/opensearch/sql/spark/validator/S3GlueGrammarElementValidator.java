/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.opensearch.sql.spark.validator.GrammarElement.ALTER_VIEW;
import static org.opensearch.sql.spark.validator.GrammarElement.BITWISE_FUNCTIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.CLUSTER_BY;
import static org.opensearch.sql.spark.validator.GrammarElement.CREATE_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.CREATE_VIEW;
import static org.opensearch.sql.spark.validator.GrammarElement.CROSS_JOIN;
import static org.opensearch.sql.spark.validator.GrammarElement.DESCRIBE_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.DISTRIBUTE_BY;
import static org.opensearch.sql.spark.validator.GrammarElement.DROP_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.DROP_VIEW;
import static org.opensearch.sql.spark.validator.GrammarElement.FILE;
import static org.opensearch.sql.spark.validator.GrammarElement.FULL_OUTER_JOIN;
import static org.opensearch.sql.spark.validator.GrammarElement.HINTS;
import static org.opensearch.sql.spark.validator.GrammarElement.INLINE_TABLE;
import static org.opensearch.sql.spark.validator.GrammarElement.INSERT;
import static org.opensearch.sql.spark.validator.GrammarElement.LEFT_ANTI_JOIN;
import static org.opensearch.sql.spark.validator.GrammarElement.LEFT_SEMI_JOIN;
import static org.opensearch.sql.spark.validator.GrammarElement.LOAD;
import static org.opensearch.sql.spark.validator.GrammarElement.MANAGE_RESOURCE;
import static org.opensearch.sql.spark.validator.GrammarElement.MISC_FUNCTIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.REFRESH_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.REFRESH_RESOURCE;
import static org.opensearch.sql.spark.validator.GrammarElement.RESET;
import static org.opensearch.sql.spark.validator.GrammarElement.RIGHT_OUTER_JOIN;
import static org.opensearch.sql.spark.validator.GrammarElement.SET;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_FUNCTIONS;
import static org.opensearch.sql.spark.validator.GrammarElement.SHOW_VIEWS;
import static org.opensearch.sql.spark.validator.GrammarElement.TABLESAMPLE;
import static org.opensearch.sql.spark.validator.GrammarElement.TABLE_VALUED_FUNCTION;
import static org.opensearch.sql.spark.validator.GrammarElement.TRANSFORM;
import static org.opensearch.sql.spark.validator.GrammarElement.UDF;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class S3GlueGrammarElementValidator extends DenyListGrammarElementValidator {
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
              BITWISE_FUNCTIONS,
              MISC_FUNCTIONS,
              UDF)
          .build();

  public S3GlueGrammarElementValidator() {
    super(S3GLUE_DENY_LIST);
  }
}
