/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class GetDirectQueryResourcesRequestTest {

  @Test
  public void testDefaultConstructor() {
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    assertNull(request.getDataSource());
    assertNull(request.getResourceType());
    assertNull(request.getResourceName());
    assertNull(request.getQueryParams());
  }

  @Test
  public void testSettersAndGetters() {
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    
    request.setDataSource("prometheus");
    assertEquals("prometheus", request.getDataSource());
    
    request.setResourceType(DirectQueryResourceType.LABELS);
    assertEquals(DirectQueryResourceType.LABELS, request.getResourceType());
    
    request.setResourceName("test-resource");
    assertEquals("test-resource", request.getResourceName());
    
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("param1", "value1");
    request.setQueryParams(queryParams);
    assertEquals(queryParams, request.getQueryParams());
  }

  @Test
  public void testSetResourceTypeFromString() {
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    
    request.setResourceTypeFromString("labels");
    assertEquals(DirectQueryResourceType.LABELS, request.getResourceType());
    
    request.setResourceTypeFromString("METADATA");
    assertEquals(DirectQueryResourceType.METADATA, request.getResourceType());
    
    request.setResourceTypeFromString("series");
    assertEquals(DirectQueryResourceType.SERIES, request.getResourceType());
  }

  @Test
  public void testSetResourceTypeFromStringNull() {
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.LABELS);
    
    request.setResourceTypeFromString(null);
    assertEquals(DirectQueryResourceType.LABELS, request.getResourceType());
  }

  @Test
  public void testLombokDataAnnotation() {
    GetDirectQueryResourcesRequest request1 = new GetDirectQueryResourcesRequest();
    request1.setDataSource("prometheus");
    request1.setResourceType(DirectQueryResourceType.LABELS);
    
    GetDirectQueryResourcesRequest request2 = new GetDirectQueryResourcesRequest();
    request2.setDataSource("prometheus");
    request2.setResourceType(DirectQueryResourceType.LABELS);
    
    assertEquals(request1, request2);
    assertEquals(request1.hashCode(), request2.hashCode());
    assertEquals(request1.toString(), request2.toString());
  }
}