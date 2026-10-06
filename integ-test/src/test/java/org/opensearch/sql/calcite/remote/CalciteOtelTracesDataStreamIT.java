/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.util.OtelDataStream;

/** Schema evolution across the backing indices of an OTel traces data stream. */
public class CalciteOtelTracesDataStreamIT extends OtelDataStreamConflictTestCase {

  @Override
  protected OtelDataStream create(String name) throws IOException {
    return OtelDataStream.traces(client(), name);
  }

  @Override
  protected String stableField() {
    return "serviceName";
  }

  @Test
  public void typeDrifts() throws IOException {
    runDrifts("otel-traces-drift");
  }
}
