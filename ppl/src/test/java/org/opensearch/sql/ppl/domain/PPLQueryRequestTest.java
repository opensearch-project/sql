/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.ppl.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

}
