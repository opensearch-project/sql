/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
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
    // Given
    StatementModel statementModel =
        StatementModel.builder()
            .version("1.0")
            .statementState(StatementState.RUNNING)
            .statementId(new StatementId("statement1"))
            .sessionId(new SessionId("session1"))
            .applicationId("app1")
            .jobId("job1")
            .langType(LangType.SQL)
            .datasourceName("datasource1")
            .query("SELECT * FROM table")
            .queryId("query1")
            .submitTime(System.currentTimeMillis())
            .error(null)
            .build();

    // When
    XContentBuilder xContentBuilder =
        serializer.toXContent(statementModel, ToXContent.EMPTY_PARAMS);
    String json = xContentBuilder.toString();

    assertEquals(true, json.contains("\"version\":\"1.0\""));
    assertEquals(true, json.contains("\"state\":\"running\""));
    assertEquals(true, json.contains("\"statementId\":\"statement1\""));
  }

  @Test
  void fromXContentShouldDeserializeStatementModel() throws Exception {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();
    // Given
    String json =
        "{\"version\":\"1.0\",\"type\":\"statement\",\"state\":\"running\",\"statementId\":\"statement1\",\"sessionId\":\"session1\",\"applicationId\":\"app1\",\"jobId\":\"job1\",\"lang\":\"SQL\",\"dataSourceName\":\"datasource1\",\"query\":\"SELECT"
            + " * FROM table\",\"queryId\":\"query1\",\"submitTime\":1623456789,\"error\":\"\"}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    StatementModel statementModel = serializer.fromXContent(parser, 1L, 1L);

    assertEquals("1.0", statementModel.getVersion());
    assertEquals(StatementState.RUNNING, statementModel.getStatementState());
    assertEquals("statement1", statementModel.getStatementId().getId());
    assertEquals("session1", statementModel.getSessionId().getSessionId());
  }

  @Test
  void fromXContentShouldDeserializeStatementModelThrowException() throws Exception {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();
    // Given
    String json =
        "{\"version\":\"1.0\",\"type\":\"statement_state\",\"state\":\"running\",\"statementId\":\"statement1\",\"sessionId\":\"session1\",\"applicationId\":\"app1\",\"jobId\":\"job1\",\"lang\":\"SQL\",\"dataSourceName\":\"datasource1\",\"query\":\"SELECT"
            + " * FROM table\",\"queryId\":\"query1\",\"submitTime\":1623456789,\"error\":null}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();

    assertThrows(IllegalStateException.class, () -> serializer.fromXContent(parser, 1L, 1L));
  }

  @Test
  void fromXContentThrowsExceptionWhenParsingInvalidContent() {
    XContentParser parser = mock(XContentParser.class);

    assertThrows(RuntimeException.class, () -> serializer.fromXContent(parser, 0, 0));
  }

  @Test
  void fromXContentShouldThrowExceptionForUnexpectedField() throws Exception {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();

    String jsonWithUnexpectedField =
        "{\"version\":\"1.0\",\"type\":\"statement\",\"state\":\"running\",\"statementId\":\"statement1\",\"sessionId\":\"session1\",\"applicationId\":\"app1\",\"jobId\":\"job1\",\"lang\":\"SQL\",\"dataSourceName\":\"datasource1\",\"query\":\"SELECT"
            + " * FROM"
            + " table\",\"queryId\":\"query1\",\"submitTime\":1623456789,\"error\":\"\",\"unexpectedField\":\"someValue\"}";
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                jsonWithUnexpectedField);
    parser.nextToken();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> serializer.fromXContent(parser, 1L, 1L));
    assertEquals("Unexpected field: unexpectedField", exception.getMessage());
  }
}
