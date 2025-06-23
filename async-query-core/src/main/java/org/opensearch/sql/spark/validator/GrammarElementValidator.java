/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

/** Interface for validator to decide if each GrammarElement is valid or not. */
public interface GrammarElementValidator {

  /**
   * @return true if element is valid (accepted)
   */
  boolean isValid(GrammarElement element);
}
