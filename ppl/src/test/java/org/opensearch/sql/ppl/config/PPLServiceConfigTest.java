/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.config;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.opensearch.sql.ppl.PPLService;

public class PPLServiceConfigTest {
  @Test
  public void testConfigPPLServiceShouldPass() {
    PPLServiceConfig config = new PPLServiceConfig();
    PPLService service = config.pplService();
    assertNotNull(service);
  }
}
