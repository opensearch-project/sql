/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockSettings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesRequest;

/*
 * @opensearch.experimental
 */
public class DirectQueryResourcesRequestConverterTest {

  private RestRequest request;
  private MockSettings mockSettings;

  @BeforeEach
  public void setup() {
    // allow mocking final methods
    mockSettings = withSettings().mockMaker("mock-maker-inline");
    request = mock(RestRequest.class, mockSettings);
  }

  @Test
  public void testToGetDirectRestRequestForStandardEndpoint() {
    // Setup
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("labels");
    when(request.param("resourceName")).thenReturn("testLabel");
    when(request.param("start")).thenReturn("2023-01-01");
    when(request.param("end")).thenReturn("2023-01-02");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/api/v1/labels/testLabel/values");

    Map<String, String> params =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "labels",
            "resourceName", "testLabel",
            "start", "2023-01-01",
            "end", "2023-01-02");

    when(request.params()).thenReturn(ImmutableMap.copyOf(params));
    when(request.consumedParams())
        .thenReturn(List.of("dataSource", "resourceType", "resourceName"));

    // Execute
    GetDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toGetDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.LABELS, result.getResourceType());
    assertEquals("testLabel", result.getResourceName());
    assertEquals(Map.of("start", "2023-01-01", "end", "2023-01-02"), result.getQueryParams());
  }

  @Test
  public void testToGetDirectRestRequestForAlertmanagerAlertsEndpoint() {
    // Setup
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("alerts");
    when(request.param("silenced")).thenReturn("false");
    when(request.param("active")).thenReturn("true");
    when(request.path())
        .thenReturn("/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/alerts");

    Map<String, String> params =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "alerts",
            "silenced", "false",
            "active", "true");

    when(request.params()).thenReturn(ImmutableMap.copyOf(params));
    when(request.consumedParams()).thenReturn(List.of("dataSource", "resourceType"));

    // Execute
    GetDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toGetDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.ALERTMANAGER_ALERTS, result.getResourceType());
    assertEquals(Map.of("silenced", "false", "active", "true"), result.getQueryParams());
  }

  @Test
  public void testToGetDirectRestRequestForAlertmanagerAlertGroupsEndpoint() {
    // Setup
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("silenced")).thenReturn("false");
    when(request.param("active")).thenReturn("true");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/alerts/groups");

    Map<String, String> params =
        Map.of(
            "dataSource", "testDataSource",
            "silenced", "false",
            "active", "true");

    when(request.params()).thenReturn(ImmutableMap.copyOf(params));
    when(request.consumedParams()).thenReturn(List.of("dataSource"));

    // Execute
    GetDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toGetDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.ALERTMANAGER_ALERT_GROUPS, result.getResourceType());
    assertEquals(Map.of("silenced", "false", "active", "true"), result.getQueryParams());
  }

  @Test
  public void testToGetDirectRestRequestForAlertmanagerReceiversEndpoint() {
    // Setup
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("receivers");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/receivers");

    Map<String, String> params =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "receivers");

    when(request.params()).thenReturn(ImmutableMap.copyOf(params));
    when(request.consumedParams()).thenReturn(List.of("dataSource", "resourceType"));

    // Execute
    GetDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toGetDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.ALERTMANAGER_RECEIVERS, result.getResourceType());
    assertEquals(Map.of(), result.getQueryParams());
  }

  @Test
  public void testToGetDirectRestRequestForAlertmanagerSilencesEndpoint() {
    // Setup
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("silences");
    when(request.param("filter")).thenReturn("alertname=test");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/silences");

    Map<String, String> params =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "silences",
            "filter", "alertname=test");

    when(request.params()).thenReturn(ImmutableMap.copyOf(params));
    when(request.consumedParams()).thenReturn(List.of("dataSource", "resourceType"));

    // Execute
    GetDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toGetDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.ALERTMANAGER_SILENCES, result.getResourceType());
    assertEquals(Map.of("filter", "alertname=test"), result.getQueryParams());
  }

  @Test
  public void testToWriteDirectRestRequestForAlertmanagerSilencesWithContent() {
    // Setup
    String requestBody = "{\"matchers\":[{\"name\":\"alertname\",\"value\":\"TestAlert\"}],\"comment\":\"Test silence\"}";
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("silences");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/silences");
    when(request.hasContent()).thenReturn(true);
    when(request.content()).thenReturn(new BytesArray(requestBody));

    // Execute
    WriteDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toWriteDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.ALERTMANAGER_SILENCES, result.getResourceType());
    assertEquals(requestBody, result.getRequest());
  }

  @Test
  public void testToWriteDirectRestRequestForAlertmanagerAlertsGroupsEndpoint() {
    // Setup
    String requestBody = "{\"action\":\"update\"}";
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/alerts/groups");
    when(request.hasContent()).thenReturn(true);
    when(request.content()).thenReturn(new BytesArray(requestBody));

    // Execute
    WriteDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toWriteDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.ALERTMANAGER_ALERT_GROUPS, result.getResourceType());
    assertEquals(requestBody, result.getRequest());
  }

  @Test
  public void testToWriteDirectRestRequestForStandardEndpoint() {
    // Setup
    String requestBody = "{\"data\":\"test\"}";
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("metadata");
    when(request.param("resourceName")).thenReturn("test-config");
    when(request.path())
        .thenReturn("/_plugins/_directquery/_resources/testDataSource/api/v1/configs/test-config");
    when(request.hasContent()).thenReturn(true);
    when(request.content()).thenReturn(new BytesArray(requestBody));

    // Execute
    WriteDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toWriteDirectRestRequest(request);

    // Verify
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.METADATA, result.getResourceType());
    assertEquals("test-config", result.getResourceName());
    assertEquals(requestBody, result.getRequest());
  }

  @Test
  public void testToWriteDirectRestRequestWithoutContentThrowsException() {
    // Setup
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("silences");
    when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/alertmanager/api/v2/silences");
    when(request.hasContent()).thenReturn(false);

    // Execute and Verify
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DirectQueryResourcesRequestConverter.toWriteDirectRestRequest(request));

    assertEquals("The write direct resource request must have a request in the body", exception.getMessage());
  }

  @Test
  public void testToGetDirectRestRequestWithoutResourceName() {
    // Setup - test the branch where resourceName is null
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("labels");
    when(request.param("resourceName")).thenReturn(null);  // This is the key difference
    when(request.param("start")).thenReturn("2023-01-01");
    when(request.path())
        .thenReturn("/_plugins/_directquery/_resources/testDataSource/api/v1/labels");

    Map<String, String> params =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "labels",
            "start", "2023-01-01");

    when(request.params()).thenReturn(ImmutableMap.copyOf(params));
    when(request.consumedParams())
        .thenReturn(List.of("dataSource", "resourceType"));

    // Execute
    GetDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toGetDirectRestRequest(request);

    // Verify - resourceName should be null since it wasn't provided
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.LABELS, result.getResourceType());
    assertEquals(null, result.getResourceName());  // This tests the null branch
    assertEquals(Map.of("start", "2023-01-01"), result.getQueryParams());
  }

  @Test
  public void testToWriteDirectRestRequestWithoutResourceName() {
    // Setup - test the branch where resourceName is null in write request
    String requestBody = "{\"data\":\"test\"}";
    when(request.param("dataSource")).thenReturn("testDataSource");
    when(request.param("resourceType")).thenReturn("metadata");
    when(request.param("resourceName")).thenReturn(null);
    when(request.path())
        .thenReturn("/_plugins/_directquery/_resources/testDataSource/api/v1/metadata");
    when(request.hasContent()).thenReturn(true);
    when(request.content()).thenReturn(new BytesArray(requestBody));

    // Execute
    WriteDirectQueryResourcesRequest result =
        DirectQueryResourcesRequestConverter.toWriteDirectRestRequest(request);

    // Verify - resourceName should be null since it wasn't provided
    assertEquals("testDataSource", result.getDataSource());
    assertEquals(DirectQueryResourceType.METADATA, result.getResourceType());
    assertEquals(null, result.getResourceName());
    assertEquals(requestBody, result.getRequest());
  }
}
