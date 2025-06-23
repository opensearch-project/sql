/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.opensearch.sql.spark.validator.SQLGrammarElement.ALTER_VIEW;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.BITWISE_FUNCTIONS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CLUSTER_BY;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CREATE_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CREATE_VIEW;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.CROSS_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DESCRIBE_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DISTRIBUTE_BY;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.DROP_FUNCTION;
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
import static org.opensearch.sql.spark.validator.SQLGrammarElement.RESET;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.RIGHT_OUTER_JOIN;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SET;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_FUNCTIONS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.SHOW_VIEWS;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TABLESAMPLE;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TABLE_VALUED_FUNCTION;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.TRANSFORM;
import static org.opensearch.sql.spark.validator.SQLGrammarElement.UDF;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class S3GlueSQLGrammarElementValidator extends DenyListGrammarElementValidator {
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

  public S3GlueSQLGrammarElementValidator() {
    super(S3GLUE_DENY_LIST);
  }
}
