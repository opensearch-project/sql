/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.LangType;

public class CreateAsyncQueryRequestConverterTest {

  @Test
  public void fromXContent() throws IOException {
    String request =
        "{\n"
            + "  \"datasource\": \"my_glue\",\n"
            + "  \"lang\": \"sql\",\n"
            + "  \"query\": \"select 1\"\n"
            + "}";
    CreateAsyncQueryRequest queryRequest =
        CreateAsyncQueryRequestConverter.fromXContentParser(xContentParser(request));
    Assertions.assertEquals("my_glue", queryRequest.getDatasource());
    Assertions.assertEquals(LangType.SQL, queryRequest.getLang());
    Assertions.assertEquals("select 1", queryRequest.getQuery());
  }

  @Test
  public void testConstructor() {
    Assertions.assertDoesNotThrow(
        () -> new CreateAsyncQueryRequest("select * from apple", "my_glue", LangType.SQL));
  }

  @Test
  public void fromXContentWithDuplicateFields() throws IOException {
    String request =
        "{\n"
            + "  \"datasource\": \"my_glue\",\n"
            + "  \"datasource\": \"my_glue_1\",\n"
            + "  \"lang\": \"sql\",\n"
            + "  \"query\": \"select 1\"\n"
            + "}";
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> CreateAsyncQueryRequestConverter.fromXContentParser(xContentParser(request)));
    Assertions.assertTrue(
        illegalArgumentException
            .getMessage()
            .contains("Error while parsing the request body: Duplicate field 'datasource'"));
  }

  @Test
  public void fromXContentWithUnknownField() throws IOException {
    String request =
        "{\n"
            + "  \"datasource\": \"my_glue\",\n"
            + "  \"random\": \"my_gue_1\",\n"
            + "  \"lang\": \"sql\",\n"
            + "  \"query\": \"select 1\"\n"
            + "}";
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> CreateAsyncQueryRequestConverter.fromXContentParser(xContentParser(request)));
    Assertions.assertEquals(
        "Error while parsing the request body: Unknown field: random",
        illegalArgumentException.getMessage());
  }

  @Test
  public void fromXContentWithWrongDatatype() throws IOException {
    String request =
        "{\"datasource\": [\"my_glue\", \"my_glue_1\"], \"lang\": \"sql\", \"query\": \"select"
            + " 1\"}";
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> CreateAsyncQueryRequestConverter.fromXContentParser(xContentParser(request)));
    Assertions.assertEquals(
        "Error while parsing the request body: Can't get text on a START_ARRAY at 1:16",
        illegalArgumentException.getMessage());
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
        CreateAsyncQueryRequestConverter.fromXContentParser(xContentParser(request));
    Assertions.assertEquals("00fdjevgkf12s00q", queryRequest.getSessionId());
  }

  private XContentParser xContentParser(String request) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, request);
  }
}
