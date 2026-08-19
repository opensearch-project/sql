/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage.scan;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.planner.logical.rules.PrometheusPushDownContext;
import org.opensearch.sql.prometheus.response.PrometheusResponse;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;

/**
 * Physical scan node for Prometheus metrics. Executes the actual PromQL query using the
 * accumulated pushdown state from {@link PrometheusPushDownContext}.
 */
public class CalciteEnumerablePrometheusScan extends TableScan
    implements Scannable, EnumerableRel {

  @Getter private final PrometheusMetricTable prometheusTable;
  @Getter private final PrometheusPushDownContext pushDownContext;
  @Getter private final RelDataType schema;

  public CalciteEnumerablePrometheusScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      PrometheusMetricTable prometheusTable,
      RelDataType schema,
      PrometheusPushDownContext pushDownContext) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.prometheusTable = prometheusTable;
    this.schema = schema;
    this.pushDownContext = pushDownContext;
  }

  @Override
  public RelDataType deriveRowType() {
    return schema;
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

    Expression scanOperator = implementor.stash(this, CalciteEnumerablePrometheusScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  @Override
  public Enumerable<@Nullable Object> scan() {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<@Nullable Object> enumerator() {
        return new PrometheusEnumerator();
      }
    };
  }

  /** Enumerator that executes the PromQL query and iterates over results. */
  private class PrometheusEnumerator implements Enumerator<@Nullable Object> {
    private Iterator<ExprValue> responseIterator;
    private List<String> fieldOrder;
    private Map<String, ExprType> fieldTypes;
    private Object current;

    PrometheusEnumerator() {
      try {
        PrometheusClient client = prometheusTable.getPrometheusClient();
        String metricName = prometheusTable.getMetricName();

        String promQL;
        long startTime;
        long endTime;
        String step;

        if (prometheusTable.getPrometheusQueryRequest() != null) {
          // Use pre-configured query request (from query_range table function)
          var request = prometheusTable.getPrometheusQueryRequest();
          promQL = request.getPromQl();
          startTime = request.getStartTime();
          endTime = request.getEndTime();
          step = request.getStep();
        } else {
          // Build PromQL from pushdown context
          promQL = pushDownContext.buildPromQL(metricName);
          startTime = pushDownContext.getEffectiveStartTime();
          endTime = pushDownContext.getEffectiveEndTime();
          step = pushDownContext.getEffectiveStep();
        }

        JSONObject responseObject = client.queryRange(promQL, startTime, endTime, step);
        PrometheusResponseFieldNames fieldNames = new PrometheusResponseFieldNames();
        PrometheusResponse response = new PrometheusResponse(responseObject, fieldNames);

        this.fieldTypes = prometheusTable.getFieldTypes();
        this.fieldOrder = new ArrayList<>(fieldTypes.keySet());
        this.responseIterator = response.iterator();
      } catch (IOException e) {
        throw new RuntimeException(
            "Error fetching data from Prometheus server: " + e.getMessage(), e);
      }
    }

    @Override
    public Object current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      if (responseIterator.hasNext()) {
        ExprValue exprValue = responseIterator.next();
        Map<String, ExprValue> tupleValue = exprValue.tupleValue();

        if (fieldOrder.size() == 1) {
          // Single column — Calcite expects a scalar value
          String fieldName = fieldOrder.get(0);
          ExprValue value = tupleValue.get(fieldName);
          current = convertExprValueToCalcite(value, fieldTypes.get(fieldName));
        } else {
          // Multiple columns — Calcite expects Object[]
          Object[] row = new Object[fieldOrder.size()];
          for (int i = 0; i < fieldOrder.size(); i++) {
            String fieldName = fieldOrder.get(i);
            ExprValue value = tupleValue.get(fieldName);
            row[i] = convertExprValueToCalcite(value, fieldTypes.get(fieldName));
          }
          current = row;
        }
        return true;
      }
      return false;
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException("Reset not supported for Prometheus scan");
    }

    @Override
    public void close() {
      // No resources to close
    }

    private Object convertExprValueToCalcite(ExprValue value, ExprType type) {
      if (value == null) {
        return null;
      }
      if (type == ExprCoreType.TIMESTAMP) {
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
        return value.value().toString();
      }
    }
  }
}
