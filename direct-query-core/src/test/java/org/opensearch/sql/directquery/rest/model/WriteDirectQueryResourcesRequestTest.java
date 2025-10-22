/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class WriteDirectQueryResourcesRequestTest {

  @Test
  public void testDefaultConstructor() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    assertNull(request.getDataSource());
    assertNull(request.getResourceType());
    assertNull(request.getResourceName());
    assertNull(request.getRequest());
    assertNull(request.getRequestOptions());
  }

  @Test
  public void testSettersAndGetters() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();

    request.setDataSource("prometheus");
    assertEquals("prometheus", request.getDataSource());

    request.setResourceType(DirectQueryResourceType.RULES);
    assertEquals(DirectQueryResourceType.RULES, request.getResourceType());

    request.setResourceName("test-rule");
    assertEquals("test-rule", request.getResourceName());

    request.setRequest("rule-body");
    assertEquals("rule-body", request.getRequest());

    Map<String, String> requestOptions = new HashMap<>();
    requestOptions.put("option1", "value1");
    request.setRequestOptions(requestOptions);
    assertEquals(requestOptions, request.getRequestOptions());
  }

  @Test
  public void testSetResourceTypeFromString() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();

    request.setResourceTypeFromString("rules");
    assertEquals(DirectQueryResourceType.RULES, request.getResourceType());

    request.setResourceTypeFromString("ALERTS");
    assertEquals(DirectQueryResourceType.ALERTS, request.getResourceType());

    request.setResourceTypeFromString("alertmanager_silences");
    assertEquals(DirectQueryResourceType.ALERTMANAGER_SILENCES, request.getResourceType());
  }

  @Test
  public void testSetResourceTypeFromStringNull() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.RULES);

    request.setResourceTypeFromString(null);
    assertEquals(DirectQueryResourceType.RULES, request.getResourceType());
  }

  @Test
  public void testLombokDataAnnotation() {
    WriteDirectQueryResourcesRequest request1 = new WriteDirectQueryResourcesRequest();
    request1.setDataSource("prometheus");
    request1.setResourceType(DirectQueryResourceType.RULES);
    request1.setResourceName("test-rule");

    WriteDirectQueryResourcesRequest request2 = new WriteDirectQueryResourcesRequest();
    request2.setDataSource("prometheus");
    request2.setResourceType(DirectQueryResourceType.RULES);
    request2.setResourceName("test-rule");

    assertEquals(request1, request2);
    assertEquals(request1.hashCode(), request2.hashCode());
    assertEquals(request1.toString(), request2.toString());
  }

  @Test
  public void testEqualsAndHashCodeDifferentValues() {
    WriteDirectQueryResourcesRequest request1 = new WriteDirectQueryResourcesRequest();
    request1.setDataSource("prometheus");
    request1.setResourceType(DirectQueryResourceType.RULES);

    WriteDirectQueryResourcesRequest request2 = new WriteDirectQueryResourcesRequest();
    request2.setDataSource("prometheus");
    request2.setResourceType(DirectQueryResourceType.ALERTS);

    assertNotEquals(request1, request2);
    assertNotEquals(request1.hashCode(), request2.hashCode());
  }

  @Test
  public void testToStringContainsValues() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource("prometheus");
    request.setResourceType(DirectQueryResourceType.METADATA);
    request.setResourceName("cpu_usage");

    String toString = request.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("prometheus"));
    assertTrue(toString.contains("METADATA"));
    assertTrue(toString.contains("cpu_usage"));
  }

  @Test
  public void testWithEmptyRequestOptions() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    Map<String, String> emptyOptions = new HashMap<>();
    request.setRequestOptions(emptyOptions);

    assertEquals(emptyOptions, request.getRequestOptions());
    assertTrue(request.getRequestOptions().isEmpty());
  }

  @Test
  public void testSetResourceTypeFromStringCaseInsensitive() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();

    request.setResourceTypeFromString("metadata");
    assertEquals(DirectQueryResourceType.METADATA, request.getResourceType());

    request.setResourceTypeFromString("LABELS");
    assertEquals(DirectQueryResourceType.LABELS, request.getResourceType());

    request.setResourceTypeFromString("AlErts");
    assertEquals(DirectQueryResourceType.ALERTS, request.getResourceType());
  }

  @Test
  public void testSetAllResourceTypes() {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();

    request.setResourceTypeFromString("metadata");
    assertEquals(DirectQueryResourceType.METADATA, request.getResourceType());

    request.setResourceTypeFromString("LABELS");
    assertEquals(DirectQueryResourceType.LABELS, request.getResourceType());

    request.setResourceTypeFromString("label");
    assertEquals(DirectQueryResourceType.LABEL, request.getResourceType());

    request.setResourceTypeFromString("RULES");
    assertEquals(DirectQueryResourceType.RULES, request.getResourceType());

    request.setResourceTypeFromString("ALERTS");
    assertEquals(DirectQueryResourceType.ALERTS, request.getResourceType());

    request.setResourceTypeFromString("ALERTMANAGER_SILENCES");
    assertEquals(DirectQueryResourceType.ALERTMANAGER_SILENCES, request.getResourceType());
  }
}