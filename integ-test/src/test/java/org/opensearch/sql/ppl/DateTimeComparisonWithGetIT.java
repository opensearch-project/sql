/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import org.opensearch.rest.RestRequest;

/** Run DateTimeComparisonIT tests using http GET request */
public class DateTimeComparisonWithGetIT extends DateTimeComparisonIT {
  public DateTimeComparisonWithGetIT(String functionCall, String name, Boolean expectedResult) {
    super(functionCall, name, expectedResult);
  }

  @Override
  public void init() throws IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }
}
