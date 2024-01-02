/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import org.opensearch.rest.RestRequest;

/** Run ShowDataSourcesCommandIT tests using http GET request */
public class ShowDataSourcesCommandWithGetIT extends ShowDataSourcesCommandIT {
  @Override
  protected void init() throws InterruptedException, IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }
}
