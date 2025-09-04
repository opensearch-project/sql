/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.query;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.datasource.client.DataSourceClient;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.exception.PrometheusClientException;
import org.opensearch.sql.prometheus.model.MetricMetadata;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;

public class PrometheusQueryHandler implements QueryHandler<PrometheusClient> {
  private static final Logger LOG = LogManager.getLogger(PrometheusQueryHandler.class);

  @Override
  public DataSourceType getSupportedDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }

  @Override
  public boolean canHandle(DataSourceClient client) {
    return client instanceof PrometheusClient;
  }

  @Override
  public Class<PrometheusClient> getClientClass() {
    return PrometheusClient.class;
  }

  @Override
  public String executeQuery(PrometheusClient client, ExecuteDirectQueryRequest request)
      throws IOException {
    return AccessController.doPrivileged(
        (PrivilegedAction<String>)
            () -> {
              try {
                PrometheusOptions options = request.getPrometheusOptions();
                PrometheusQueryType queryType = options.getQueryType();
                if (queryType == null) {
                  return createErrorJson("Query type is required for Prometheus queries");
                }

                String startTimeStr = options.getStart();
                String endTimeStr = options.getEnd();
                Integer limit = request.getMaxResults();
                Integer timeout = request.getTimeout();

                if (queryType == PrometheusQueryType.RANGE
                    && (startTimeStr == null || endTimeStr == null)) {
                  return createErrorJson("Start and end times are required for Prometheus queries");
                } else if (queryType == PrometheusQueryType.INSTANT && options.getTime() == null) {
                  return createErrorJson("Time is required for instant Prometheus queries");
                }

                switch (queryType) {
                  case RANGE:
                    {
                      JSONObject metricData =
                          client.queryRange(
                              request.getQuery(),
                              Long.parseLong(startTimeStr),
                              Long.parseLong(endTimeStr),
                              options.getStep(),
                              limit,
                              timeout);
                      return metricData.toString();
                    }

                  case INSTANT:
                  default:
                    {
                      JSONObject metricData =
                          client.query(
                              request.getQuery(),
                              Long.parseLong(options.getTime()),
                              limit,
                              timeout);
                      return metricData.toString();
                    }
                }
              } catch (NumberFormatException e) {
                return createErrorJson("Invalid time format: " + e.getMessage());
              } catch (PrometheusClientException e) {
                LOG.error("Prometheus client error executing query", e);
                return createErrorJson(e.getMessage());
              } catch (IOException e) {
                LOG.error("Error executing query", e);
                return createErrorJson(e.getMessage());
              }
            });
  }

  private String createErrorJson(String message) {
    return new JSONObject().put("error", message).toString();
  }

  @Override
  public GetDirectQueryResourcesResponse<?> getResources(
      PrometheusClient client, GetDirectQueryResourcesRequest request) {
    return AccessController.doPrivileged(
        (PrivilegedAction<GetDirectQueryResourcesResponse<?>>)
            () -> {
              try {
                if (request.getResourceType() == null) {
                  throw new IllegalArgumentException("Resource type cannot be null");
                }

                switch (request.getResourceType()) {
                  case LABELS:
                    {
                      List<String> labels = client.getLabels(request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withStringList(labels);
                    }
                  case LABEL:
                    {
                      List<String> labelValues =
                          client.getLabel(request.getResourceName(), request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withStringList(labelValues);
                    }
                  case METADATA:
                    {
                      Map<String, List<MetricMetadata>> metadata =
                          client.getAllMetrics(request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withMap(metadata);
                    }
                  case SERIES:
                    {
                      List<Map<String, String>> series = client.getSeries(request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withList(series);
                    }
                  case ALERTS:
                    {
                      JSONObject alerts = client.getAlerts();
                      return GetDirectQueryResourcesResponse.withMap(alerts.toMap());
                    }
                  case RULES:
                    {
                      JSONObject rules = client.getRules(request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withMap(rules.toMap());
                    }
                  case ALERTMANAGER_ALERTS:
                    {
                      JSONArray alerts = client.getAlertmanagerAlerts(request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withList(alerts.toList());
                    }
                  case ALERTMANAGER_ALERT_GROUPS:
                    {
                      JSONArray alertGroups =
                          client.getAlertmanagerAlertGroups(request.getQueryParams());
                      return GetDirectQueryResourcesResponse.withList(alertGroups.toList());
                    }
                  case ALERTMANAGER_RECEIVERS:
                    {
                      JSONArray receivers = client.getAlertmanagerReceivers();
                      return GetDirectQueryResourcesResponse.withList(receivers.toList());
                    }
                  case ALERTMANAGER_SILENCES:
                    {
                      JSONArray silences = client.getAlertmanagerSilences();
                      return GetDirectQueryResourcesResponse.withList(silences.toList());
                    }
                  default:
                    throw new IllegalArgumentException(
                        "Invalid prometheus resource type: " + request.getResourceType());
                }
              } catch (IOException e) {
                LOG.error("Error getting resources", e);
                throw new org.opensearch.sql.prometheus.exception.PrometheusClientException(
                    String.format(
                        "Error while getting resources for %s: %s",
                        request.getResourceType(), e.getMessage()));
              }
            });
  }
}
