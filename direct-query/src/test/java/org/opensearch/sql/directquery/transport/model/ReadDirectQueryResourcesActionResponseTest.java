/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/*
 * @opensearch.experimental
 */
public class ReadDirectQueryResourcesActionResponseTest {

  @Test
  public void testConstructorWithResult() {
    String testResult = "Successfully retrieved resources";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(testResult);

    assertEquals(testResult, response.getResult());
  }

  @Test
  public void testConstructorWithEmptyResult() {
    String emptyResult = "";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(emptyResult);

    assertEquals(emptyResult, response.getResult());
  }

  @Test
  public void testStreamSerializationWithResult() throws IOException {
    String testResult = "Test query execution successful";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(testResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(testResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithEmptyResult() throws IOException {
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse("");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals("", deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithJsonResult() throws IOException {
    String jsonResult = "{\"data\": [{\"metric\": \"cpu_usage\", \"value\": 85.2}, {\"metric\": \"memory_usage\", \"value\": 67.8}]}";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(jsonResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(jsonResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithLargeResult() throws IOException {
    StringBuilder largeResultBuilder = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      largeResultBuilder.append("Resource item ").append(i).append(" with detailed metadata. ");
    }
    String largeResult = largeResultBuilder.toString();

    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(largeResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(largeResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithSpecialChars() throws IOException {
    String specialCharsResult = "Results with symbols: !@#$%^&*()_+-=[]{}|;':\",./<>?";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(specialCharsResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(specialCharsResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithMultilineResult() throws IOException {
    String multilineResult = "Resource List:\nMetric 1: cpu_usage\r\nMetric 2: memory_usage\n\tDetails:\n  - Instance: web-server-1\n  - Value: 75.3%";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(multilineResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(multilineResult, deserializedResponse.getResult());
  }

  @Test
  public void testGetterMethod() {
    String testResult = "Getter test for resources";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(testResult);

    assertNotNull(response.getResult());
    assertEquals(testResult, response.getResult());
  }

  @Test
  public void testResponseWithErrorMessage() throws IOException {
    String errorResult = "Error: Unable to fetch resources - Connection timeout";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(errorResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(errorResult, deserializedResponse.getResult());
  }

  @Test
  public void testResponseWithPrometheusData() throws IOException {
    String prometheusResult = "# TYPE cpu_usage gauge\ncpu_usage{instance=\"localhost:9090\"} 45.2\n# TYPE memory_usage gauge\nmemory_usage{instance=\"localhost:9090\"} 67.8";
    ReadDirectQueryResourcesActionResponse response =
        new ReadDirectQueryResourcesActionResponse(prometheusResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ReadDirectQueryResourcesActionResponse deserializedResponse =
        new ReadDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(prometheusResult, deserializedResponse.getResult());
  }
}