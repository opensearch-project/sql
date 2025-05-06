/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

public class DefaultGrammarElementValidator implements GrammarElementValidator {
  @Override
  public boolean isValid(GrammarElement element) {
    return true;
  }
}
