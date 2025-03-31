/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.validator;

import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.spark.rest.model.LangType;

public class DirectQueryRequestValidator {
  private DirectQueryRequestValidator() {}

  public static void validateRequest(ExecuteDirectQueryRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("Request cannot be null");
    }

    if (request.getDataSources() == null || request.getDataSources().isEmpty()) {
      throw new IllegalArgumentException("Datasource is required");
    }

    if (request.getQuery() == null || request.getQuery().isEmpty()) {
      throw new IllegalArgumentException("Query is required");
    }

    if (request.getLanguage() == null) {
      throw new IllegalArgumentException("Language type is required");
    }

    if (request.getLanguage() == LangType.PROMQL) {
      PrometheusOptions prometheusOptions = request.getPrometheusOptions();
      if (prometheusOptions.getQueryType() == null) {
        throw new IllegalArgumentException("Prometheus options are required for PROMQL queries");
      }

      // Validate based on query type
      switch (prometheusOptions.getQueryType()) {
        case PrometheusQueryType.RANGE:
          String start = prometheusOptions.getStart();
          String end = prometheusOptions.getEnd();

          if (start == null || end == null) {
            throw new IllegalArgumentException(
                "Start and end times are required for range queries");
          }

          // Validate step parameter
          if (prometheusOptions.getStep() == null || prometheusOptions.getStep().isEmpty()) {
            throw new IllegalArgumentException("Step parameter is required for range queries");
          }

          // Validate timestamps
          try {
            long startTimestamp = Long.parseLong(start);
            long endTimestamp = Long.parseLong(end);
            if (endTimestamp <= startTimestamp) {
              throw new IllegalArgumentException("End time must be after start time");
            }
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid time format: start and end must be numeric timestamps");
          }
          break;

        case PrometheusQueryType.INSTANT:
        default: // should not happen. Replace with switch expression when dropping JDK11 support
          // For instant queries, validate time parameter
          if (prometheusOptions.getTime() == null) {
            throw new IllegalArgumentException("Time parameter is required for instant queries");
          }

          // Validate time format
          try {
            Long.parseLong(prometheusOptions.getTime());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid time format: time must be a numeric timestamp");
          }
          break;
      }
    }
  }
}
