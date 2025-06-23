/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import java.util.Set;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DenyListGrammarElementValidator implements GrammarElementValidator {
  private final Set<GrammarElement> denyList;

  @Override
  public boolean isValid(GrammarElement element) {
    return !denyList.contains(element);
  }
}
