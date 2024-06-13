/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.session.SessionType;

class SessionModelXContentSerializerTest {

  private final SessionModelXContentSerializer serializer = new SessionModelXContentSerializer();

  @Test
  void toXContentShouldSerializeSessionModel() throws Exception {
    SessionModel sessionModel =
        SessionModel.builder()
            .version("1.0")
            .sessionType(SessionType.INTERACTIVE)
            .sessionId("session1")
            .sessionState(SessionState.FAIL)
            .datasourceName("datasource1")
            .accountId("account1")
            .applicationId("app1")
            .jobId("job1")
            .lastUpdateTime(System.currentTimeMillis())
            .error(null)
            .build();

    XContentBuilder xContentBuilder = serializer.toXContent(sessionModel, ToXContent.EMPTY_PARAMS);

    String json = xContentBuilder.toString();
    assertEquals(true, json.contains("\"version\":\"1.0\""));
    assertEquals(true, json.contains("\"type\":\"session\""));
    assertEquals(true, json.contains("\"sessionType\":\"interactive\""));
    assertEquals(true, json.contains("\"sessionId\":\"session1\""));
    assertEquals(true, json.contains("\"state\":\"fail\""));
    assertEquals(true, json.contains("\"dataSourceName\":\"datasource1\""));
    assertEquals(true, json.contains("\"accountId\":\"account1\""));
    assertEquals(true, json.contains("\"applicationId\":\"app1\""));
    assertEquals(true, json.contains("\"jobId\":\"job1\""));
  }

  @Test
  void fromXContentShouldDeserializeSessionModel() throws Exception {
    String json = getBaseJson().toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    SessionModel sessionModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("1.0", sessionModel.getVersion());
    assertEquals(SessionType.INTERACTIVE, sessionModel.getSessionType());
    assertEquals("session1", sessionModel.getSessionId());
    assertEquals(SessionState.FAIL, sessionModel.getSessionState());
    assertEquals("datasource1", sessionModel.getDatasourceName());
    assertEquals("account1", sessionModel.getAccountId());
    assertEquals("app1", sessionModel.getApplicationId());
    assertEquals("job1", sessionModel.getJobId());
  }

  @Test
  void fromXContentShouldDeserializeSessionModelWithoutAccountId() throws Exception {
    String json = getJsonWithout("accountId").toString();
    XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    SessionModel sessionModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("1.0", sessionModel.getVersion());
    assertEquals(SessionType.INTERACTIVE, sessionModel.getSessionType());
    assertEquals("session1", sessionModel.getSessionId());
    assertEquals(SessionState.FAIL, sessionModel.getSessionState());
    assertEquals("datasource1", sessionModel.getDatasourceName());
    assertNull(sessionModel.getAccountId());
    assertEquals("app1", sessionModel.getApplicationId());
    assertEquals("job1", sessionModel.getJobId());
  }

  private JSONObject getJsonWithout(String attr) {
    JSONObject result = getBaseJson();
    result.remove(attr);
    return result;
  }

  private JSONObject getBaseJson() {
    return new JSONObject()
        .put("version", "1.0")
        .put("type", "session")
        .put("sessionType", "interactive")
        .put("sessionId", "session1")
        .put("state", "fail")
        .put("dataSourceName", "datasource1")
        .put("accountId", "account1")
        .put("applicationId", "app1")
        .put("jobId", "job1")
        .put("lastUpdateTime", 1623456789)
        .put("error", "");
  }

  @Test
  void fromXContentThrowsExceptionWhenParsingInvalidContent() {
    XContentParser parser = mock(XContentParser.class);

    assertThrows(RuntimeException.class, () -> serializer.fromXContent(parser, 0, 0));
  }
}
