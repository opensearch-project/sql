/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.validator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.spark.rest.model.LangType;

public class DirectQueryRequestValidatorTest {

  @Test
  public void testValidateNullRequest() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(null));
    assertEquals("Request cannot be null", exception.getMessage());
  }

  @Test
  public void testValidateRequestWithNullDataSource() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Datasource is required", exception.getMessage());
  }

  @Test
  public void testValidateRequestWithEmptyDataSource() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Datasource is required", exception.getMessage());
  }

  @Test
  public void testValidateRequestWithNullQuery() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setLanguage(LangType.PROMQL);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Query is required", exception.getMessage());
  }

  @Test
  public void testValidateRequestWithEmptyQuery() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("");
    request.setLanguage(LangType.PROMQL);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Query is required", exception.getMessage());
  }

  @Test
  public void testValidateRequestWithNullLanguage() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Language type is required", exception.getMessage());
  }

  @Test
  public void testValidateRequestWithSqlLanguage() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setLanguage(LangType.SQL);
    request.setQuery("select 1");

    assertDoesNotThrow(() -> DirectQueryRequestValidator.validateRequest(request));
  }

  @Test
  public void testValidatePromQLRequestWithoutOptions() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Prometheus options are required for PROMQL queries", exception.getMessage());
  }

  @Test
  public void testValidatePromQLRangeQueryMissingStartEnd() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStep("15s");
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Start and end times are required for range queries", exception.getMessage());
  }

  @Test
  public void testValidatePromQLRangeQueryMissingEnd() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStep("15s");
    options.setStart("now");
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Start and end times are required for range queries", exception.getMessage());
  }

  @Test
  public void testValidatePromQLRangeQueryMissingStep() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStart("1609459200");
    options.setEnd("1609545600");
    // Missing step
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Step parameter is required for range queries", exception.getMessage());
  }

  @Test
  public void testValidatePromQLRangeQueryEmptyStep() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStart("1609459200");
    options.setEnd("1609545600");
    options.setStep("");
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Step parameter is required for range queries", exception.getMessage());
  }

  @Test
  public void testValidatePromQLRangeQueryInvalidTimeFormat() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStart("invalid-time");
    options.setEnd("1609545600");
    options.setStep("15s");
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals(
        "Invalid time format: start and end must be numeric timestamps", exception.getMessage());
  }

  @Test
  public void testValidatePromQLRangeQueryEndBeforeStart() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStart("1609545600"); // End time
    options.setEnd("1609459200"); // Start time (which is earlier)
    options.setStep("15s");
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("End time must be after start time", exception.getMessage());
  }

  @Test
  public void testValidatePromQLInstantQueryMissingTime() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    // Missing time
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Time parameter is required for instant queries", exception.getMessage());
  }

  @Test
  public void testValidatePromQLInstantQueryInvalidTimeFormat() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("invalid-time");
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Invalid time format: time must be a numeric timestamp", exception.getMessage());
  }

  @Test
  public void testValidatePromQLQueryNullQueryType() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(null);
    request.setPrometheusOptions(options);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DirectQueryRequestValidator.validateRequest(request));
    assertEquals("Prometheus options are required for PROMQL queries", exception.getMessage());
  }

  @Test
  public void testValidateValidPromQLRangeQuery() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStart("1609459200"); // 2021-01-01
    options.setEnd("1609545600"); // 2021-01-02
    options.setStep("15s");
    request.setPrometheusOptions(options);

    assertDoesNotThrow(() -> DirectQueryRequestValidator.validateRequest(request));
  }

  @Test
  public void testValidateValidPromQLInstantQuery() {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources("prometheus");
    request.setQuery("up");
    request.setLanguage(LangType.PROMQL);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("1609459200"); // 2021-01-01
    request.setPrometheusOptions(options);

    assertDoesNotThrow(() -> DirectQueryRequestValidator.validateRequest(request));
  }
}
