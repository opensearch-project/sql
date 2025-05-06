/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;

class IndexDMLResultXContentSerializerTest {

  private final IndexDMLResultXContentSerializer serializer =
      new IndexDMLResultXContentSerializer();

  @Test
  void toXContentShouldSerializeIndexDMLResult() throws IOException {
    IndexDMLResult dmlResult =
        IndexDMLResult.builder()
            .queryId("query1")
            .status("SUCCESS")
            .error(null)
            .datasourceName("datasource1")
            .queryRunTime(1000L)
            .updateTime(2000L)
            .build();

    XContentBuilder xContentBuilder = serializer.toXContent(dmlResult, ToXContent.EMPTY_PARAMS);
    String json = xContentBuilder.toString();

    assertTrue(json.contains("\"queryId\":\"query1\""));
    assertTrue(json.contains("\"status\":\"SUCCESS\""));
    assertTrue(json.contains("\"error\":null"));
    assertTrue(json.contains("\"dataSourceName\":\"datasource1\""));
    assertTrue(json.contains("\"queryRunTime\":1000"));
    assertTrue(json.contains("\"updateTime\":2000"));
    assertTrue(json.contains("\"result\":[]"));
    assertTrue(json.contains("\"schema\":[]"));
  }

  @Test
  void toXContentShouldHandleErrorInIndexDMLResult() throws IOException {
    IndexDMLResult dmlResult =
        IndexDMLResult.builder()
            .queryId("query1")
            .status("FAILURE")
            .error("An error occurred")
            .datasourceName("datasource1")
            .queryRunTime(1000L)
            .updateTime(2000L)
            .build();

    XContentBuilder xContentBuilder = serializer.toXContent(dmlResult, ToXContent.EMPTY_PARAMS);

    String json = xContentBuilder.toString();
    assertTrue(json.contains("\"queryId\":\"query1\""));
    assertTrue(json.contains("\"status\":\"FAILURE\""));
    assertTrue(json.contains("\"error\":\"An error occurred\""));
    assertTrue(json.contains("\"dataSourceName\":\"datasource1\""));
    assertTrue(json.contains("\"queryRunTime\":1000"));
    assertTrue(json.contains("\"updateTime\":2000"));
    assertTrue(json.contains("\"result\":[]"));
    assertTrue(json.contains("\"schema\":[]"));
  }

  @Test
  void fromXContentShouldThrowUnsupportedOperationException() {
    assertThrows(UnsupportedOperationException.class, () -> serializer.fromXContent(null, 0L, 0L));
  }
}
