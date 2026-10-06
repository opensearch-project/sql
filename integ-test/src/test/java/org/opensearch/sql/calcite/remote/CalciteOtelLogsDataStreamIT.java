/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.util.OtelDataStream;

/** Schema evolution across the backing indices of an OTel logs data stream. */
public class CalciteOtelLogsDataStreamIT extends OtelDataStreamConflictTestCase {

  @Override
  protected OtelDataStream create(String name) throws IOException {
    return OtelDataStream.logs(client(), name);
  }

  @Override
  protected String control() {
    return "`resource.attributes.service.name`";
  }

  @Test
  public void typeDrifts() throws IOException {
    runDrifts("otel-logs-drift");
  }
}
