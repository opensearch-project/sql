/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.JobType;

class AsyncQueryJobMetadataXContentSerializerTest {

  private final AsyncQueryJobMetadataXContentSerializer serializer =
      new AsyncQueryJobMetadataXContentSerializer();

  @Test
  void toXContentShouldSerializeAsyncQueryJobMetadata() throws Exception {
    AsyncQueryJobMetadata jobMetadata =
        AsyncQueryJobMetadata.builder()
            .queryId("query1")
            .accountId("account1")
            .applicationId("app1")
            .jobId("job1")
            .resultIndex("result1")
            .sessionId("session1")
            .datasourceName("datasource1")
            .jobType(JobType.INTERACTIVE)
            .indexName("index1")
            .metadata(XContentSerializerUtil.buildMetadata(1L, 1L))
            .build();

    XContentBuilder xContentBuilder = serializer.toXContent(jobMetadata, ToXContent.EMPTY_PARAMS);
    String json = xContentBuilder.toString();

    assertEquals(true, json.contains("\"queryId\":\"query1\""));
    assertEquals(true, json.contains("\"type\":\"jobmeta\""));
    assertEquals(true, json.contains("\"jobId\":\"job1\""));
    assertEquals(true, json.contains("\"accountId\":\"account1\""));
    assertEquals(true, json.contains("\"applicationId\":\"app1\""));
    assertEquals(true, json.contains("\"resultIndex\":\"result1\""));
    assertEquals(true, json.contains("\"sessionId\":\"session1\""));
    assertEquals(true, json.contains("\"dataSourceName\":\"datasource1\""));
    assertEquals(true, json.contains("\"jobType\":\"interactive\""));
    assertEquals(true, json.contains("\"indexName\":\"index1\""));
  }

  @Test
  void fromXContentShouldDeserializeAsyncQueryJobMetadata() throws Exception {
    String json = getBaseJson().toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    AsyncQueryJobMetadata jobMetadata = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("query1", jobMetadata.getQueryId());
    assertEquals("job1", jobMetadata.getJobId());
    assertEquals("account1", jobMetadata.getAccountId());
    assertEquals("app1", jobMetadata.getApplicationId());
    assertEquals("result1", jobMetadata.getResultIndex());
    assertEquals("session1", jobMetadata.getSessionId());
    assertEquals("datasource1", jobMetadata.getDatasourceName());
    assertEquals(JobType.INTERACTIVE, jobMetadata.getJobType());
    assertEquals("index1", jobMetadata.getIndexName());
  }

  @Test
  void fromXContentShouldThrowExceptionWhenMissingJobId() throws Exception {
    String json = getJsonWithout("jobId").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentShouldThrowExceptionWhenMissingApplicationId() throws Exception {
    String json = getJsonWithout("applicationId").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentShouldThrowExceptionWhenUnknownFields() throws Exception {
    String json = getBaseJson().put("unknownAttr", "index1").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentShouldDeserializeAsyncQueryWithJobTypeNUll() throws Exception {
    String json = getBaseJson().put("jobType", "").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    AsyncQueryJobMetadata jobMetadata = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("query1", jobMetadata.getQueryId());
    assertEquals("job1", jobMetadata.getJobId());
    assertEquals("account1", jobMetadata.getAccountId());
    assertEquals("app1", jobMetadata.getApplicationId());
    assertEquals("result1", jobMetadata.getResultIndex());
    assertEquals("session1", jobMetadata.getSessionId());
    assertEquals("datasource1", jobMetadata.getDatasourceName());
    assertNull(jobMetadata.getJobType());
    assertEquals("index1", jobMetadata.getIndexName());
  }

  @Test
  void fromXContentShouldDeserializeAsyncQueryWithAccountIdNUll() throws Exception {
    String json = getJsonWithout("accountId").put("jobType", "").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    AsyncQueryJobMetadata jobMetadata = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("query1", jobMetadata.getQueryId());
    assertEquals("job1", jobMetadata.getJobId());
    assertEquals("app1", jobMetadata.getApplicationId());
    assertEquals("result1", jobMetadata.getResultIndex());
    assertEquals("session1", jobMetadata.getSessionId());
    assertEquals("datasource1", jobMetadata.getDatasourceName());
    assertNull(jobMetadata.getJobType());
    assertEquals("index1", jobMetadata.getIndexName());
  }

  @Test
  void fromXContentShouldDeserializeAsyncQueryWithoutJobId() throws Exception {
    String json = getJsonWithout("jobId").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  private JSONObject getJsonWithout(String... attrs) {
    JSONObject result = getBaseJson();
    for (String attr : attrs) {
      result.remove(attr);
    }
    return result;
  }

  private JSONObject getBaseJson() {
    return new JSONObject()
        .put("queryId", "query1")
        .put("type", "jobmeta")
        .put("jobId", "job1")
        .put("accountId", "account1")
        .put("applicationId", "app1")
        .put("resultIndex", "result1")
        .put("sessionId", "session1")
        .put("dataSourceName", "datasource1")
        .put("jobType", "interactive")
        .put("indexName", "index1");
  }
}
