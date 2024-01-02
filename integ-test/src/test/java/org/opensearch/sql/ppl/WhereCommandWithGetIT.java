/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;

/** Run WhereCommandIT tests using http GET request */
public class WhereCommandWithGetIT extends WhereCommandIT {

  @Override
  public void init() throws IOException {
    super.init();
    this.method = GET;
  }
}
