/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum PPLGrammarElement implements GrammarElement {
  PATTERNS_COMMAND("patterns command"),
  JOIN_COMMAND("join command"),
  LOOKUP_COMMAND("lookup command"),
  SUBQUERY_COMMAND("subquery command"),
  FLATTEN_COMMAND("flatten command"),
  FILLNULL_COMMAND("fillnull command"),
  EXPAND_COMMAND("expand command"),
  DESCRIBE_COMMAND("describe command"),
  IPADDRESS_FUNCTIONS("IP address functions"),
  JSON_FUNCTIONS("JSON functions"),
  LAMBDA_FUNCTIONS("Lambda functions");

  String description;

  @Override
  public String toString() {
    return description;
  }
}
