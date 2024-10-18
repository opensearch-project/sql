/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.opensearch.rest.RestRequest;

/** Run MetricsIT tests using http GET request */
public class MetricsWithGetIT extends MetricsIT {
  @Override
  public void init() throws Exception {
    super.init();
    this.method = RestRequest.Method.GET;
  }
}
