/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.session.SessionType;

class SessionModelXContentSerializerTest {

  private final SessionModelXContentSerializer serializer = new SessionModelXContentSerializer();

  @Test
  void toXContentShouldSerializeSessionModel() throws Exception {
    // Given
    SessionModel sessionModel =
        SessionModel.builder()
            .version("1.0")
            .sessionType(SessionType.INTERACTIVE)
            .sessionId(new SessionId("session1"))
            .sessionState(SessionState.FAIL)
            .datasourceName("datasource1")
            .applicationId("app1")
            .jobId("job1")
            .lastUpdateTime(System.currentTimeMillis())
            .error(null)
            .build();

    // When
    XContentBuilder xContentBuilder = serializer.toXContent(sessionModel, ToXContent.EMPTY_PARAMS);
    String json = xContentBuilder.toString();

    // Then
    assertEquals(true, json.contains("\"version\":\"1.0\""));
    assertEquals(true, json.contains("\"type\":\"session\""));
    assertEquals(true, json.contains("\"sessionType\":\"interactive\""));
    assertEquals(true, json.contains("\"sessionId\":\"session1\""));
    assertEquals(true, json.contains("\"state\":\"fail\""));
    assertEquals(true, json.contains("\"dataSourceName\":\"datasource1\""));
    assertEquals(true, json.contains("\"applicationId\":\"app1\""));
    assertEquals(true, json.contains("\"jobId\":\"job1\""));
  }

  @Test
  void fromXContentShouldDeserializeSessionModel() throws Exception {
    // Given
    String json =
        "{\n"
            + "  \"version\": \"1.0\",\n"
            + "  \"type\": \"session\",\n"
            + "  \"sessionType\": \"interactive\",\n"
            + "  \"sessionId\": \"session1\",\n"
            + "  \"state\": \"fail\",\n"
            + "  \"dataSourceName\": \"datasource1\",\n"
            + "  \"applicationId\": \"app1\",\n"
            + "  \"jobId\": \"job1\",\n"
            + "  \"lastUpdateTime\": 1623456789,\n"
            + "  \"error\": \"\"\n"
            + "}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    // When
    SessionModel sessionModel = serializer.fromXContent(parser, 1L, 1L);

    // Then
    assertEquals("1.0", sessionModel.getVersion());
    assertEquals(SessionType.INTERACTIVE, sessionModel.getSessionType());
    assertEquals("session1", sessionModel.getSessionId().getSessionId());
    assertEquals(SessionState.FAIL, sessionModel.getSessionState());
    assertEquals("datasource1", sessionModel.getDatasourceName());
    assertEquals("app1", sessionModel.getApplicationId());
    assertEquals("job1", sessionModel.getJobId());
  }

  @Test
  void fromXContentThrowsExceptionWhenParsingInvalidContent() {
    XContentParser parser = mock(XContentParser.class);

    assertThrows(RuntimeException.class, () -> serializer.fromXContent(parser, 0, 0));
  }
}
