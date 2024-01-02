/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import org.opensearch.rest.RestRequest;

/** Run SimpleQueryStringIT tests using http GET request */
public class SimpleQueryStringWithGetIT extends SimpleQueryStringIT {
  @Override
  public void init() throws IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }
}
