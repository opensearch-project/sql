/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

public interface GrammarElementValidator {
  boolean isValid(GrammarElement element);
}
