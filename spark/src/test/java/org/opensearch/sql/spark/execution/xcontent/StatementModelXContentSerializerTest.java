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
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
class StatementModelXContentSerializerTest {

  private StatementModelXContentSerializer serializer;

  @Test
  void toXContentShouldSerializeStatementModel() throws Exception {
    serializer = new StatementModelXContentSerializer();
    StatementModel statementModel =
        StatementModel.builder()
            .version("1.0")
            .statementState(StatementState.RUNNING)
            .statementId(new StatementId("statement1"))
            .sessionId(new SessionId("session1"))
            .accountId("account1")
            .applicationId("app1")
            .jobId("job1")
            .langType(LangType.SQL)
            .datasourceName("datasource1")
            .query("SELECT * FROM table")
            .queryId("query1")
            .submitTime(System.currentTimeMillis())
            .error(null)
            .build();

    XContentBuilder xContentBuilder =
        serializer.toXContent(statementModel, ToXContent.EMPTY_PARAMS);

    String json = xContentBuilder.toString();
    assertEquals(true, json.contains("\"version\":\"1.0\""));
    assertEquals(true, json.contains("\"state\":\"running\""));
    assertEquals(true, json.contains("\"statementId\":\"statement1\""));
    assertEquals(true, json.contains("\"accountId\":\"account1\""));
    assertEquals(true, json.contains("\"applicationId\":\"app1\""));
    assertEquals(true, json.contains("\"jobId\":\"job1\""));
  }

  @Test
  void fromXContentShouldDeserializeStatementModel() throws Exception {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();
    String json = getBaseJson().toString();
    final XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    StatementModel statementModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("1.0", statementModel.getVersion());
    assertEquals(StatementState.RUNNING, statementModel.getStatementState());
    assertEquals("statement1", statementModel.getStatementId().getId());
    assertEquals("session1", statementModel.getSessionId().getSessionId());
    assertEquals("account1", statementModel.getAccountId());
  }

  @Test
  void fromXContentShouldDeserializeStatementModelWithoutAccountId() throws Exception {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();
    String json = getJsonWithout("accountId").toString();
    final XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    StatementModel statementModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("1.0", statementModel.getVersion());
    assertEquals(StatementState.RUNNING, statementModel.getStatementState());
    assertEquals("statement1", statementModel.getStatementId().getId());
    assertEquals("session1", statementModel.getSessionId().getSessionId());
    assertNull(statementModel.getAccountId());
  }

  @Test
  void fromXContentThrowsExceptionWhenParsingInvalidContent() {
    XContentParser parser = mock(XContentParser.class);

    assertThrows(RuntimeException.class, () -> serializer.fromXContent(parser, 0, 0));
  }

  @Test
  void fromXContentShouldThrowExceptionForUnexpectedField() throws Exception {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();
    String json = getBaseJson().put("unexpectedField", "someValue").toString();
    final XContentParser parser = XContentSerializerTestUtil.prepareParser(json);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
    assertEquals("Unexpected field: unexpectedField", exception.getMessage());
  }

  private JSONObject getJsonWithout(String attr) {
    JSONObject result = getBaseJson();
    result.remove(attr);
    return result;
  }

  private JSONObject getBaseJson() {
    return new JSONObject()
        .put("version", "1.0")
        .put("type", "statement")
        .put("state", "running")
        .put("statementId", "statement1")
        .put("sessionId", "session1")
        .put("accountId", "account1")
        .put("applicationId", "app1")
        .put("jobId", "job1")
        .put("lang", "SQL")
        .put("dataSourceName", "datasource1")
        .put("query", "SELECT * FROM table")
        .put("queryId", "query1")
        .put("submitTime", 1623456789)
        .put("error", "");
  }
}
