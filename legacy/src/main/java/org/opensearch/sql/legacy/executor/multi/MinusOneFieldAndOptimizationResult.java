/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.multi;

import java.util.Set;
import org.opensearch.search.SearchHit;

/** Created by Eliran on 26/8/2016. */
class MinusOneFieldAndOptimizationResult {
  private Set<Object> fieldValues;
  private SearchHit someHit;

  MinusOneFieldAndOptimizationResult(Set<Object> fieldValues, SearchHit someHit) {
    this.fieldValues = fieldValues;
    this.someHit = someHit;
  }

  public Set<Object> getFieldValues() {
    return fieldValues;
  }

  public SearchHit getSomeHit() {
    return someHit;
  }
}
