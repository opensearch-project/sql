/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.protocol.response.format.Format;

public class PPLQueryRequestTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void getRequestShouldPass() {
    PPLQueryRequest request = new PPLQueryRequest("source=t a=1", null, null);
    request.getRequest();
  }

  @Test
  public void testExplainRequest() {
    PPLQueryRequest request = new PPLQueryRequest(
        "source=t a=1", null, "/_plugins/_ppl/_explain");
    assertTrue(request.isExplainRequest());
  }

  @Test
  public void testDefaultFormat() {
    PPLQueryRequest request = new PPLQueryRequest(
        "source=test", null, "/_plugins/_ppl");
    assertEquals(request.format(), Format.JDBC);
  }

  @Test
  public void testJDBCFormat() {
    PPLQueryRequest request = new PPLQueryRequest(
        "source=test", null, "/_plugins/_ppl", "jdbc");
    assertEquals(request.format(), Format.JDBC);
  }

  @Test
  public void testCSVFormat() {
    PPLQueryRequest request = new PPLQueryRequest(
        "source=test", null, "/_plugins/_ppl", "csv");
    assertEquals(request.format(), Format.CSV);
  }

  @Test
  public void testUnsupportedFormat() {
    String format = "notsupport";
    PPLQueryRequest request = new PPLQueryRequest(
            "source=test", null, "/_plugins/_ppl", format);
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("response in " + format + " format is not supported.");
    request.format();
  }

  @Test
  public void testValidate() {
    PPLQueryRequest request = new PPLQueryRequest("source=t a=1", null, null);
    assertNull(request.validate());
  }

  @Test
  public void testPPLQueryRequestFromActionRequest() {
    PPLQueryRequest request = new PPLQueryRequest("source=t a=1", null, null);
    assertEquals(PPLQueryRequest.fromActionRequest(request), request);
  }

  @Test
  public void testCustomizedNonNullJSONContentActionRequestFromActionRequest() {
    PPLQueryRequest request =
        new PPLQueryRequest("source=t a=1", new JSONObject("{\"query\":\"source=t a=1\"}"), null);
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
    PPLQueryRequest recreatedObject = PPLQueryRequest.fromActionRequest(actionRequest);
    assertNotSame(request, recreatedObject);
    assertEquals(request.getRequest(), recreatedObject.getRequest());
  }

  @Test
  public void testCustomizedNullJSONContentActionRequestFromActionRequest() {
    PPLQueryRequest request = new PPLQueryRequest("source=t a=1", null, null);
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
    PPLQueryRequest recreatedObject = PPLQueryRequest.fromActionRequest(actionRequest);
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
    exceptionRule.expectMessage("failed to parse ActionRequest into PPLQueryRequest");
    PPLQueryRequest.fromActionRequest(actionRequest);
  }
}
