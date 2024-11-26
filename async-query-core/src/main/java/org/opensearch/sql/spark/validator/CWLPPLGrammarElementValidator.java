/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.opensearch.sql.spark.validator.PPLGrammarElement.DESCRIBE_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.EXPAND_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.FILLNULL_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.FLATTEN_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.IPADDRESS_FUNCTIONS;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.JOIN_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.JSON_FUNCTIONS;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.LAMBDA_FUNCTIONS;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.LOOKUP_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.PATTERNS_COMMAND;
import static org.opensearch.sql.spark.validator.PPLGrammarElement.SUBQUERY_COMMAND;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class CWLPPLGrammarElementValidator extends DenyListGrammarElementValidator {
  private static final Set<GrammarElement> CWL_DENY_LIST =
      ImmutableSet.<GrammarElement>builder()
          .add(
              PATTERNS_COMMAND,
              JOIN_COMMAND,
              LOOKUP_COMMAND,
              SUBQUERY_COMMAND,
              FLATTEN_COMMAND,
              FILLNULL_COMMAND,
              EXPAND_COMMAND,
              DESCRIBE_COMMAND,
              IPADDRESS_FUNCTIONS,
              JSON_FUNCTIONS,
              LAMBDA_FUNCTIONS)
          .build();

  public CWLPPLGrammarElementValidator() {
    super(CWL_DENY_LIST);
  }
}
