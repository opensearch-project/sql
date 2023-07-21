/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamOutput;

public class TransportPPLQueryRequestTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testValidate() {
    TransportPPLQueryRequest request = new TransportPPLQueryRequest("source=t a=1", null, null);
    assertNull(request.validate());
  }

  @Test
  public void testTransportPPLQueryRequestFromActionRequest() {
    TransportPPLQueryRequest request = new TransportPPLQueryRequest("source=t a=1", null, null);
    assertEquals(TransportPPLQueryRequest.fromActionRequest(request), request);
  }

  @Test
  public void testCustomizedNonNullJSONContentActionRequestFromActionRequest() {
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest(
            "source=t a=1", new JSONObject("{\"query\":\"source=t a=1\"}"), null);
    ActionRequest actionRequest =
        new ActionRequest() {
          @Override
          public ActionRequestValidationException validate() {
            return null;
          }

          @Override
          public void writeTo(StreamOutput out) throws IOException {
            request.writeTo(out);
          }
        };
    TransportPPLQueryRequest recreatedObject =
        TransportPPLQueryRequest.fromActionRequest(actionRequest);
    assertNotSame(request, recreatedObject);
    assertEquals(request.getRequest(), recreatedObject.getRequest());
  }

  @Test
  public void testCustomizedNullJSONContentActionRequestFromActionRequest() {
    TransportPPLQueryRequest request = new TransportPPLQueryRequest(
            "source=t a=1", null, null
    );
    ActionRequest actionRequest =
        new ActionRequest() {
          @Override
          public ActionRequestValidationException validate() {
            return null;
          }

          @Override
          public void writeTo(StreamOutput out) throws IOException {
            request.writeTo(out);
          }
        };
    TransportPPLQueryRequest recreatedObject =
        TransportPPLQueryRequest.fromActionRequest(actionRequest);
    assertNotSame(request, recreatedObject);
    assertEquals(request.getRequest(), recreatedObject.getRequest());
  }

  @Test
  public void testFailedParsingActionRequestFromActionRequest() {
    ActionRequest actionRequest =
        new ActionRequest() {
          @Override
          public ActionRequestValidationException validate() {
            return null;
          }

          @Override
          public void writeTo(StreamOutput out) throws IOException {
            out.writeString("sample");
          }
        };
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("failed to parse ActionRequest into TransportPPLQueryRequest");
    TransportPPLQueryRequest.fromActionRequest(actionRequest);
  }
}
