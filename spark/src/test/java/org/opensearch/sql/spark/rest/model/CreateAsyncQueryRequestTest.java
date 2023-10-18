/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest.model;

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

public class CreateAsyncQueryRequestTest {

  @Test
  public void fromXContent() throws IOException {
    String request =
        "{\n"
            + "  \"datasource\": \"my_glue\",\n"
            + "  \"lang\": \"sql\",\n"
            + "  \"query\": \"select 1\"\n"
            + "}";
    CreateAsyncQueryRequest queryRequest =
        CreateAsyncQueryRequest.fromXContentParser(xContentParser(request));
    Assertions.assertEquals("my_glue", queryRequest.getDatasource());
    Assertions.assertEquals(LangType.SQL, queryRequest.getLang());
    Assertions.assertEquals("select 1", queryRequest.getQuery());
  }

  @Test
  public void fromXContentWithSessionId() throws IOException {
    String request =
        "{\n"
            + "  \"datasource\": \"my_glue\",\n"
            + "  \"lang\": \"sql\",\n"
            + "  \"query\": \"select 1\",\n"
            + "  \"sessionId\": \"00fdjevgkf12s00q\"\n"
            + "}";
    CreateAsyncQueryRequest queryRequest =
        CreateAsyncQueryRequest.fromXContentParser(xContentParser(request));
    Assertions.assertEquals("00fdjevgkf12s00q", queryRequest.getSessionId());
  }

  private XContentParser xContentParser(String request) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, request);
  }
}
