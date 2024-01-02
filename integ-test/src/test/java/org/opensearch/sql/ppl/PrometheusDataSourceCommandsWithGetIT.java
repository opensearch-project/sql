/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import org.opensearch.rest.RestRequest;

/** Run PrometheusDataSourceCommandsIT tests using http GET request */
public class PrometheusDataSourceCommandsWithGetIT extends PrometheusDataSourceCommandsIT {
  @Override
  protected void init() throws InterruptedException, IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }
}
