/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesRequest;

/*
 * @opensearch.experimental
 */
public class WriteDirectQueryResourcesActionRequestTest {

  @Test
  public void testConstructorWithRequest() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("prometheus");
    request.setResourceType(DirectQueryResourceType.RULES);
    request.setResourceName("test-rule");
    request.setRequest("rule-body");

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    assertNotNull(actionRequest.getDirectQueryRequest());
    assertEquals("prometheus", actionRequest.getDirectQueryRequest().getDataSource());
    assertEquals(DirectQueryResourceType.RULES, actionRequest.getDirectQueryRequest().getResourceType());
    assertEquals("test-rule", actionRequest.getDirectQueryRequest().getResourceName());
    assertEquals("rule-body", actionRequest.getDirectQueryRequest().getRequest());
  }

  @Test
  public void testConstructorWithRequestAndOptions() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("prometheus");
    request.setResourceType(DirectQueryResourceType.ALERTS);

    Map<String, String> options = new HashMap<>();
    options.put("option1", "value1");
    options.put("option2", "value2");
    request.setRequestOptions(options);

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    assertNotNull(actionRequest.getDirectQueryRequest().getRequestOptions());
    assertEquals("value1", actionRequest.getDirectQueryRequest().getRequestOptions().get("option1"));
    assertEquals("value2", actionRequest.getDirectQueryRequest().getRequestOptions().get("option2"));
  }

  @Test
  public void testStreamSerializationWithAllFields() throws IOException {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("prometheus");
    request.setResourceType(DirectQueryResourceType.ALERTS);
    request.setResourceName("test-alert");
    request.setRequest("alert-body");

    Map<String, String> options = new HashMap<>();
    options.put("key1", "value1");
    options.put("key2", "value2");
    request.setRequestOptions(options);

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    BytesStreamOutput outputStream = new BytesStreamOutput();
    actionRequest.writeTo(outputStream);
    outputStream.flush();

    BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().toBytesRef().bytes);
    WriteDirectQueryResourcesActionRequest deserializedRequest =
        new WriteDirectQueryResourcesActionRequest(inputStream);
    inputStream.close();

    assertEquals("prometheus", deserializedRequest.getDirectQueryRequest().getDataSource());
    assertEquals(DirectQueryResourceType.ALERTS, deserializedRequest.getDirectQueryRequest().getResourceType());
    assertEquals("test-alert", deserializedRequest.getDirectQueryRequest().getResourceName());
    assertEquals("alert-body", deserializedRequest.getDirectQueryRequest().getRequest());
    assertEquals(2, deserializedRequest.getDirectQueryRequest().getRequestOptions().size());
    assertEquals("value1", deserializedRequest.getDirectQueryRequest().getRequestOptions().get("key1"));
    assertEquals("value2", deserializedRequest.getDirectQueryRequest().getRequestOptions().get("key2"));
  }

  @Test
  public void testStreamSerializationWithNullFields() throws IOException {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    BytesStreamOutput outputStream = new BytesStreamOutput();
    actionRequest.writeTo(outputStream);
    outputStream.flush();

    BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().toBytesRef().bytes);
    WriteDirectQueryResourcesActionRequest deserializedRequest =
        new WriteDirectQueryResourcesActionRequest(inputStream);
    inputStream.close();

    assertNull(deserializedRequest.getDirectQueryRequest().getDataSource());
    assertNull(deserializedRequest.getDirectQueryRequest().getResourceType());
    assertNull(deserializedRequest.getDirectQueryRequest().getResourceName());
    assertNull(deserializedRequest.getDirectQueryRequest().getRequest());
    assertNotNull(deserializedRequest.getDirectQueryRequest().getRequestOptions());
  }

  @Test
  public void testStreamSerializationWithPartialFields() throws IOException {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("prometheus");
    request.setResourceType(DirectQueryResourceType.LABELS);

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    BytesStreamOutput outputStream = new BytesStreamOutput();
    actionRequest.writeTo(outputStream);
    outputStream.flush();

    BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().toBytesRef().bytes);
    WriteDirectQueryResourcesActionRequest deserializedRequest =
        new WriteDirectQueryResourcesActionRequest(inputStream);
    inputStream.close();

    assertEquals("prometheus", deserializedRequest.getDirectQueryRequest().getDataSource());
    assertEquals(DirectQueryResourceType.LABELS, deserializedRequest.getDirectQueryRequest().getResourceType());
    assertNull(deserializedRequest.getDirectQueryRequest().getResourceName());
    assertNull(deserializedRequest.getDirectQueryRequest().getRequest());
  }

  @Test
  public void testStreamSerializationWithEmptyOptions() throws IOException {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("test-datasource");
    request.setRequestOptions(new HashMap<>());

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    BytesStreamOutput outputStream = new BytesStreamOutput();
    actionRequest.writeTo(outputStream);
    outputStream.flush();

    BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().toBytesRef().bytes);
    WriteDirectQueryResourcesActionRequest deserializedRequest =
        new WriteDirectQueryResourcesActionRequest(inputStream);
    inputStream.close();

    assertEquals("test-datasource", deserializedRequest.getDirectQueryRequest().getDataSource());
    assertNotNull(deserializedRequest.getDirectQueryRequest().getRequestOptions());
    assertEquals(0, deserializedRequest.getDirectQueryRequest().getRequestOptions().size());
  }

  @Test
  public void testValidate() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    assertNull(actionRequest.validate());
  }

  @Test
  public void testAllResourceTypes() throws IOException {
    DirectQueryResourceType[] resourceTypes = {
        DirectQueryResourceType.METADATA,
        DirectQueryResourceType.LABELS,
        DirectQueryResourceType.LABEL,
        DirectQueryResourceType.RULES,
        DirectQueryResourceType.ALERTS,
        DirectQueryResourceType.ALERTMANAGER_SILENCES
    };

    for (DirectQueryResourceType resourceType : resourceTypes) {
      WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
      request.setDataSource("test");
      request.setResourceType(resourceType);

      WriteDirectQueryResourcesActionRequest actionRequest =
          new WriteDirectQueryResourcesActionRequest(request);

      BytesStreamOutput outputStream = new BytesStreamOutput();
      actionRequest.writeTo(outputStream);
      outputStream.flush();

      BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().toBytesRef().bytes);
      WriteDirectQueryResourcesActionRequest deserializedRequest =
          new WriteDirectQueryResourcesActionRequest(inputStream);
      inputStream.close();

      assertEquals(resourceType, deserializedRequest.getDirectQueryRequest().getResourceType());
    }
  }

  @Test
  public void testLargeRequestOptions() throws IOException {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("prometheus");

    Map<String, String> largeOptions = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      largeOptions.put("key" + i, "value" + i);
    }
    request.setRequestOptions(largeOptions);

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(request);

    BytesStreamOutput outputStream = new BytesStreamOutput();
    actionRequest.writeTo(outputStream);
    outputStream.flush();

    BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().toBytesRef().bytes);
    WriteDirectQueryResourcesActionRequest deserializedRequest =
        new WriteDirectQueryResourcesActionRequest(inputStream);
    inputStream.close();

    assertEquals(100, deserializedRequest.getDirectQueryRequest().getRequestOptions().size());
    assertEquals("value50", deserializedRequest.getDirectQueryRequest().getRequestOptions().get("key50"));
  }
}