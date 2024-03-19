/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;

/** Run ConvertTZFunctionIT tests using http GET request */
public class ConvertTZFunctionWithGetIT extends ConvertTZFunctionIT {
  @Override
  public void init() throws IOException {
    super.init();
    this.method = GET;
  }
}
