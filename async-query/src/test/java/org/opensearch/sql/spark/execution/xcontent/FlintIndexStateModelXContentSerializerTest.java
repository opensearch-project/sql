/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

@ExtendWith(MockitoExtension.class)
class FlintIndexStateModelXContentSerializerTest {

  private FlintIndexStateModelXContentSerializer serializer =
      new FlintIndexStateModelXContentSerializer();

  @Test
  void toXContentShouldSerializeFlintIndexStateModel() throws Exception {
    FlintIndexStateModel flintIndexStateModel =
        FlintIndexStateModel.builder()
            .indexState(FlintIndexState.ACTIVE)
            .accountId("account1")
            .applicationId("app1")
            .jobId("job1")
            .latestId("latest1")
            .datasourceName("datasource1")
            .lastUpdateTime(System.currentTimeMillis())
            .error(null)
            .build();

    XContentBuilder xContentBuilder =
        serializer.toXContent(flintIndexStateModel, ToXContent.EMPTY_PARAMS);
    String json = xContentBuilder.toString();

    assertEquals(true, json.contains("\"version\":\"1.0\""));
    assertEquals(true, json.contains("\"type\":\"flintindexstate\""));
    assertEquals(true, json.contains("\"state\":\"active\""));
    assertEquals(true, json.contains("\"accountId\":\"account1\""));
    assertEquals(true, json.contains("\"applicationId\":\"app1\""));
    assertEquals(true, json.contains("\"jobId\":\"job1\""));
    assertEquals(true, json.contains("\"latestId\":\"latest1\""));
    assertEquals(true, json.contains("\"dataSourceName\":\"datasource1\""));
  }

  @Test
  void fromXContentShouldDeserializeFlintIndexStateModel() throws Exception {
    String json = getBaseJson().toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    FlintIndexStateModel flintIndexStateModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals(FlintIndexState.ACTIVE, flintIndexStateModel.getIndexState());
    assertEquals("account1", flintIndexStateModel.getAccountId());
    assertEquals("app1", flintIndexStateModel.getApplicationId());
    assertEquals("job1", flintIndexStateModel.getJobId());
    assertEquals("latest1", flintIndexStateModel.getLatestId());
    assertEquals("datasource1", flintIndexStateModel.getDatasourceName());
  }

  @Test
  void fromXContentShouldDeserializeFlintIndexStateModelWithoutAccountId() throws Exception {
    String json = getJsonWithout("accountId").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    FlintIndexStateModel flintIndexStateModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals(FlintIndexState.ACTIVE, flintIndexStateModel.getIndexState());
    assertNull(flintIndexStateModel.getAccountId());
    assertEquals("app1", flintIndexStateModel.getApplicationId());
    assertEquals("job1", flintIndexStateModel.getJobId());
    assertEquals("latest1", flintIndexStateModel.getLatestId());
    assertEquals("datasource1", flintIndexStateModel.getDatasourceName());
  }

  private JSONObject getJsonWithout(String attr) {
    JSONObject result = getBaseJson();
    result.remove(attr);
    return result;
  }

  private JSONObject getBaseJson() {
    return new JSONObject()
        .put("version", "1.0")
        .put("type", "flintindexstate")
        .put("state", "active")
        .put("statementId", "statement1")
        .put("sessionId", "session1")
        .put("accountId", "account1")
        .put("applicationId", "app1")
        .put("jobId", "job1")
        .put("latestId", "latest1")
        .put("dataSourceName", "datasource1")
        .put("lastUpdateTime", 1623456789)
        .put("error", "");
  }

  @Test
  void fromXContentThrowsExceptionWhenParsingInvalidContent() {
    XContentParser parser = mock(XContentParser.class);

    assertThrows(RuntimeException.class, () -> serializer.fromXContent(parser, 0, 0));
  }
}
