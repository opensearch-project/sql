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
public class WriteDirectQueryResourcesActionResponseTest {

  @Test
  public void testConstructorWithResult() {
    String testResult = "Operation completed successfully";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(testResult);

    assertEquals(testResult, response.getResult());
  }

  @Test
  public void testConstructorWithEmptyResult() {
    String emptyResult = "";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(emptyResult);

    assertEquals(emptyResult, response.getResult());
  }

  @Test
  public void testStreamSerializationWithResult() throws IOException {
    String testResult = "Test operation successful";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(testResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(testResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithEmptyResult() throws IOException {
    String emptyResult = "";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(emptyResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(emptyResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithLongResult() throws IOException {
    StringBuilder longResultBuilder = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      longResultBuilder.append("This is a long result message segment ").append(i).append(". ");
    }
    String longResult = longResultBuilder.toString();

    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(longResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(longResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithSpecialCharacters() throws IOException {
    String specialCharsResult = "Special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(specialCharsResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(specialCharsResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithJsonResult() throws IOException {
    String jsonResult = "{\"status\": \"success\", \"message\": \"Resource created\", \"count\": 42}";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(jsonResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(jsonResult, deserializedResponse.getResult());
  }

  @Test
  public void testStreamSerializationWithMultilineResult() throws IOException {
    String multilineResult = "Line 1\nLine 2\r\nLine 3\n\tTabbed line\n  Spaced line";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(multilineResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(multilineResult, deserializedResponse.getResult());
  }

  @Test
  public void testGetterMethod() {
    String testResult = "Getter test result";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(testResult);

    assertNotNull(response.getResult());
    assertEquals(testResult, response.getResult());
  }

  @Test
  public void testResponseWithErrorMessage() throws IOException {
    String errorResult = "Error: Failed to process request - Invalid resource type";
    WriteDirectQueryResourcesActionResponse response =
        new WriteDirectQueryResourcesActionResponse(errorResult);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    WriteDirectQueryResourcesActionResponse deserializedResponse =
        new WriteDirectQueryResourcesActionResponse(streamInput);
    streamInput.close();

    assertEquals(errorResult, deserializedResponse.getResult());
  }
}