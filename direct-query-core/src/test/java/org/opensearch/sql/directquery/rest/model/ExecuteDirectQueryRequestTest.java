/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.spark.rest.model.LangType;

/*
 * @opensearch.experimental
 */
public class ExecuteDirectQueryRequestTest {

  @Test
  public void testDefaultConstructor() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    assertNull(request.getDataSources());
    assertNull(request.getQuery());
    assertNull(request.getLanguage());
    assertNull(request.getSourceVersion());
    assertNull(request.getMaxResults());
    assertNull(request.getTimeout());
    assertNull(request.getOptions());
    assertNull(request.getSessionId());
  }

  @Test
  public void testSettersAndGetters() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();

    request.setDataSources("prometheus-ds");
    assertEquals("prometheus-ds", request.getDataSources());

    request.setQuery("up");
    assertEquals("up", request.getQuery());

    request.setLanguage(LangType.PROMQL);
    assertEquals(LangType.PROMQL, request.getLanguage());

    request.setSourceVersion("v1");
    assertEquals("v1", request.getSourceVersion());

    request.setMaxResults(100);
    assertEquals(Integer.valueOf(100), request.getMaxResults());

    request.setTimeout(30);
    assertEquals(Integer.valueOf(30), request.getTimeout());

    request.setSessionId("session-123");
    assertEquals("session-123", request.getSessionId());
  }

  @Test
  public void testGetPrometheusOptionsWhenNull() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();

    PrometheusOptions options = request.getPrometheusOptions();
    assertNotNull(options);
    assertTrue(options instanceof PrometheusOptions);
  }

  @Test
  public void testGetPrometheusOptionsWhenSet() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    PrometheusOptions expectedOptions = new PrometheusOptions();

    request.setPrometheusOptions(expectedOptions);

    PrometheusOptions actualOptions = request.getPrometheusOptions();
    assertEquals(expectedOptions, actualOptions);
    assertEquals(expectedOptions, request.getOptions());
  }

  @Test
  public void testSetPrometheusOptions() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    PrometheusOptions options = new PrometheusOptions();

    request.setPrometheusOptions(options);

    assertEquals(options, request.getOptions());
    assertEquals(options, request.getPrometheusOptions());
  }

  @Test
  public void testLombokDataAnnotation() {
    ExecuteDirectQueryRequest request1 = new ExecuteDirectQueryRequest();
    request1.setDataSources("test-ds");
    request1.setQuery("test-query");
    request1.setLanguage(LangType.SQL);

    ExecuteDirectQueryRequest request2 = new ExecuteDirectQueryRequest();
    request2.setDataSources("test-ds");
    request2.setQuery("test-query");
    request2.setLanguage(LangType.SQL);

    assertEquals(request1, request2);
    assertEquals(request1.hashCode(), request2.hashCode());
    assertEquals(request1.toString(), request2.toString());
  }
}