/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.JobType;

class AsyncQueryJobMetadataXContentSerializerTest {

  private final AsyncQueryJobMetadataXContentSerializer serializer =
      new AsyncQueryJobMetadataXContentSerializer();

  @Test
  void toXContentShouldSerializeAsyncQueryJobMetadata() throws Exception {
    AsyncQueryJobMetadata jobMetadata =
        new AsyncQueryJobMetadata(
            new AsyncQueryId("query1"),
            "app1",
            "job1",
            "result1",
            "session1",
            "datasource1",
            JobType.INTERACTIVE,
            "index1",
            1L,
            1L);

    XContentBuilder xContentBuilder = serializer.toXContent(jobMetadata, ToXContent.EMPTY_PARAMS);
    String json = xContentBuilder.toString();

    assertEquals(true, json.contains("\"queryId\":\"query1\""));
    assertEquals(true, json.contains("\"type\":\"jobmeta\""));
    assertEquals(true, json.contains("\"jobId\":\"job1\""));
    assertEquals(true, json.contains("\"applicationId\":\"app1\""));
    assertEquals(true, json.contains("\"resultIndex\":\"result1\""));
    assertEquals(true, json.contains("\"sessionId\":\"session1\""));
    assertEquals(true, json.contains("\"dataSourceName\":\"datasource1\""));
    assertEquals(true, json.contains("\"jobType\":\"interactive\""));
    assertEquals(true, json.contains("\"indexName\":\"index1\""));
  }

  @Test
  void fromXContentShouldDeserializeAsyncQueryJobMetadata() throws Exception {
    String json =
        "{\n"
            + "  \"queryId\": \"query1\",\n"
            + "  \"type\": \"jobmeta\",\n"
            + "  \"jobId\": \"job1\",\n"
            + "  \"applicationId\": \"app1\",\n"
            + "  \"resultIndex\": \"result1\",\n"
            + "  \"sessionId\": \"session1\",\n"
            + "  \"dataSourceName\": \"datasource1\",\n"
            + "  \"jobType\": \"interactive\",\n"
            + "  \"indexName\": \"index1\"\n"
            + "}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    AsyncQueryJobMetadata jobMetadata = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("query1", jobMetadata.getQueryId().getId());
    assertEquals("job1", jobMetadata.getJobId());
    assertEquals("app1", jobMetadata.getApplicationId());
    assertEquals("result1", jobMetadata.getResultIndex());
    assertEquals("session1", jobMetadata.getSessionId());
    assertEquals("datasource1", jobMetadata.getDatasourceName());
    assertEquals(JobType.INTERACTIVE, jobMetadata.getJobType());
    assertEquals("index1", jobMetadata.getIndexName());
  }

  @Test
  void fromXContentShouldThrowExceptionWhenMissingRequiredFields() throws Exception {
    String json =
        "{\n"
            + "  \"queryId\": \"query1\",\n"
            + "  \"type\": \"asyncqueryjobmeta\",\n"
            + "  \"resultIndex\": \"result1\",\n"
            + "  \"sessionId\": \"session1\",\n"
            + "  \"dataSourceName\": \"datasource1\",\n"
            + "  \"jobType\": \"async_query\",\n"
            + "  \"indexName\": \"index1\"\n"
            + "}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentShouldDeserializeWithMissingApplicationId() throws Exception {
    String json =
        "{\n"
            + "  \"queryId\": \"query1\",\n"
            + "  \"type\": \"jobmeta\",\n"
            + "  \"jobId\": \"job1\",\n"
            + "  \"resultIndex\": \"result1\",\n"
            + "  \"sessionId\": \"session1\",\n"
            + "  \"dataSourceName\": \"datasource1\",\n"
            + "  \"jobType\": \"interactive\",\n"
            + "  \"indexName\": \"index1\"\n"
            + "}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentShouldThrowExceptionWhenUnknownFields() throws Exception {
    String json =
        "{\n"
            + "  \"queryId\": \"query1\",\n"
            + "  \"type\": \"asyncqueryjobmeta\",\n"
            + "  \"resultIndex\": \"result1\",\n"
            + "  \"sessionId\": \"session1\",\n"
            + "  \"dataSourceName\": \"datasource1\",\n"
            + "  \"jobType\": \"async_query\",\n"
            + "  \"indexame\": \"index1\"\n"
            + "}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentShouldDeserializeAsyncQueryWithJobTypeNUll() throws Exception {
    String json =
        "{\n"
            + "  \"queryId\": \"query1\",\n"
            + "  \"type\": \"jobmeta\",\n"
            + "  \"jobId\": \"job1\",\n"
            + "  \"applicationId\": \"app1\",\n"
            + "  \"resultIndex\": \"result1\",\n"
            + "  \"sessionId\": \"session1\",\n"
            + "  \"dataSourceName\": \"datasource1\",\n"
            + "  \"jobType\": \"\",\n"
            + "  \"indexName\": \"index1\"\n"
            + "}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    AsyncQueryJobMetadata jobMetadata = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("query1", jobMetadata.getQueryId().getId());
    assertEquals("job1", jobMetadata.getJobId());
    assertEquals("app1", jobMetadata.getApplicationId());
    assertEquals("result1", jobMetadata.getResultIndex());
    assertEquals("session1", jobMetadata.getSessionId());
    assertEquals("datasource1", jobMetadata.getDatasourceName());
    assertNull(jobMetadata.getJobType());
    assertEquals("index1", jobMetadata.getIndexName());
  }
}
