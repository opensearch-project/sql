/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.json.JSONObject;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.scan.QueryRangeFunctionTableScanBuilder;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalPlanOptimizerFactory;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.request.system.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.response.PrometheusResponse;
import org.opensearch.sql.prometheus.storage.implementor.PrometheusDefaultImplementor;
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Prometheus table (metric) implementation. This can be constructed from a metric Name or from
 * PrometheusQueryRequest In case of query_range table function.
 *
 * <p>Implements both the V2 engine's {@link Table} interface and Calcite's {@link ScannableTable}
 * interface, enabling this table to participate in Calcite query plans (joins, lookups, etc.) while
 * retaining V2 engine compatibility.
 */
public class PrometheusMetricTable extends AbstractTable
    implements ScannableTable, Table {

  private final PrometheusClient prometheusClient;

  @Getter private final String metricName;

  @Getter private final PrometheusQueryRequest prometheusQueryRequest;

  /** The cached mapping of field and type in index. */
  private Map<String, ExprType> cachedFieldTypes = null;

  /** Default time range duration in seconds (1 hour). */
  private static final long DEFAULT_TIME_RANGE_SECONDS = 3600;

  /** Default step interval for range queries. */
  private static final String DEFAULT_STEP = "14";

  /** Constructor only with metric name. */
  public PrometheusMetricTable(PrometheusClient prometheusService, @Nonnull String metricName) {
    this.prometheusClient = prometheusService;
    this.metricName = metricName;
    this.prometheusQueryRequest = null;
  }

  /** Constructor for entire promQl Request. */
  public PrometheusMetricTable(
      PrometheusClient prometheusService, @Nonnull PrometheusQueryRequest prometheusQueryRequest) {
    this.prometheusClient = prometheusService;
    this.metricName = null;
    this.prometheusQueryRequest = prometheusQueryRequest;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("Prometheus metric exists operation is not supported");
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException("Prometheus metric create operation is not supported");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName != null) {
        cachedFieldTypes =
            new PrometheusDescribeMetricRequest(prometheusClient, null, metricName).getFieldTypes();
      } else {
        cachedFieldTypes =
            new HashMap<>(PrometheusMetricDefaultSchema.DEFAULT_MAPPING.getMapping());
        cachedFieldTypes.put(LABELS, ExprCoreType.STRING);
      }
    }
    return cachedFieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan metricScan = new PrometheusMetricScan(prometheusClient);
    return plan.accept(new PrometheusDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return PrometheusLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  // Only handling query_range function for now.
  // we need to move PPL implementations to ScanBuilder in future.
  @Override
  public TableScanBuilder createScanBuilder() {
    if (metricName == null) {
      return new QueryRangeFunctionTableScanBuilder(prometheusClient, prometheusQueryRequest);
    } else {
      return null;
    }
  }

  // ---- Calcite ScannableTable implementation ----

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return OpenSearchTypeFactory.convertSchema(this);
  }

  /**
   * Scans the Prometheus metric and returns all rows as an Enumerable for Calcite to consume. Uses
   * a default time range of 1 hour ending at the current time when no explicit query request is
   * configured.
   */
  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    try {
      JSONObject responseObject;
      if (prometheusQueryRequest != null) {
        // Use the pre-configured query request (from query_range table function)
        responseObject =
            prometheusClient.queryRange(
                prometheusQueryRequest.getPromQl(),
                prometheusQueryRequest.getStartTime(),
                prometheusQueryRequest.getEndTime(),
                prometheusQueryRequest.getStep());
      } else {
        // Default: query the metric with a 1-hour time window
        long endTime = Instant.now().getEpochSecond();
        long startTime = endTime - DEFAULT_TIME_RANGE_SECONDS;
        responseObject =
            prometheusClient.queryRange(metricName, startTime, endTime, DEFAULT_STEP);
      }

      // Parse response using the standard Prometheus response parser
      PrometheusResponseFieldNames fieldNames = new PrometheusResponseFieldNames();
      PrometheusResponse response = new PrometheusResponse(responseObject, fieldNames);

      // Convert ExprValue rows to Object[] rows for Calcite
      List<Object[]> rows = new ArrayList<>();
      Map<String, ExprType> schema = getFieldTypes();
      List<String> fieldOrder = new ArrayList<>(schema.keySet());

      for (ExprValue exprValue : response) {
        Map<String, ExprValue> tupleValue = exprValue.tupleValue();
        Object[] row = new Object[fieldOrder.size()];
        for (int i = 0; i < fieldOrder.size(); i++) {
          String fieldName = fieldOrder.get(i);
          ExprValue value = tupleValue.get(fieldName);
          row[i] = convertExprValueToCalcite(value, schema.get(fieldName));
        }
        rows.add(row);
      }
      return Linq4j.asEnumerable(rows);
    } catch (IOException e) {
      throw new RuntimeException(
          "Error fetching data from Prometheus server: " + e.getMessage(), e);
    }
  }

  /**
   * Converts an ExprValue to a Java object that Calcite can handle in its Enumerable operators.
   */
  private Object convertExprValueToCalcite(ExprValue value, ExprType type) {
    if (value == null) {
      return null;
    }
    if (type == ExprCoreType.TIMESTAMP) {
      // Calcite expects timestamps as milliseconds since epoch
      return value.timestampValue().toEpochMilli();
    } else if (type == ExprCoreType.DOUBLE) {
      return value.doubleValue();
    } else if (type == ExprCoreType.INTEGER) {
      return value.integerValue();
    } else if (type == ExprCoreType.LONG) {
      return value.longValue();
    } else if (type == ExprCoreType.STRING) {
      return value.stringValue();
    } else {
      // Default: return string representation
      return value.value().toString();
    }
  }
}
