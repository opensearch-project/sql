/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.METRIC;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.constants.TestConstants;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;

@ExtendWith(MockitoExtension.class)
class PrometheusMetricTableTest {

  @Mock
  private PrometheusClient client;

  @Test
  @SneakyThrows
  void getFieldTypesFromMetric() {
    when(client.getLabels(TestConstants.METRIC_NAME)).thenReturn(List.of("label1", "label2"));
    PrometheusMetricTable prometheusMetricTable
        = new PrometheusMetricTable(client, TestConstants.METRIC_NAME);
    Map<String, ExprType> expectedFieldTypes = new HashMap<>();
    expectedFieldTypes.put("label1", ExprCoreType.STRING);
    expectedFieldTypes.put("label2", ExprCoreType.STRING);
    expectedFieldTypes.put(VALUE, ExprCoreType.DOUBLE);
    expectedFieldTypes.put(TIMESTAMP, ExprCoreType.TIMESTAMP);
    expectedFieldTypes.put(METRIC, ExprCoreType.STRING);

    Map<String, ExprType> fieldTypes = prometheusMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verify(client, times(1)).getLabels(TestConstants.METRIC_NAME);
    assertFalse(prometheusMetricTable.getPrometheusQueryRequest().isPresent());
    assertTrue(prometheusMetricTable.getMetricName().isPresent());
    fieldTypes = prometheusMetricTable.getFieldTypes();
    verifyNoMoreInteractions(client);
  }

  @Test
  @SneakyThrows
  void getFieldTypesFromPrometheusQueryRequest() {
    PrometheusMetricTable prometheusMetricTable
        = new PrometheusMetricTable(client, new PrometheusQueryRequest());
    Map<String, ExprType> expectedFieldTypes = new HashMap<>();
    expectedFieldTypes.put(VALUE, ExprCoreType.DOUBLE);
    expectedFieldTypes.put(TIMESTAMP, ExprCoreType.TIMESTAMP);
    expectedFieldTypes.put(METRIC, ExprCoreType.STRING);

    Map<String, ExprType> fieldTypes = prometheusMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verifyNoMoreInteractions(client);
    assertTrue(prometheusMetricTable.getPrometheusQueryRequest().isPresent());
    assertFalse(prometheusMetricTable.getMetricName().isPresent());
  }

  @Test
  void testImplement() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(
        new NamedExpression(METRIC, new ReferenceExpression(METRIC, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(relation("query_range", prometheusMetricTable),
            finalProjectList, null));


    assertTrue(plan instanceof ProjectOperator);
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(METRIC, TIMESTAMP, VALUE), outputFields);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusMetricScan prometheusMetricScan =
        (PrometheusMetricScan) ((ProjectOperator) plan).getInput();
    assertEquals(prometheusQueryRequest, prometheusMetricScan.getRequest());
  }

  @Test
  void testOptimize() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(
        new NamedExpression(METRIC, new ReferenceExpression(METRIC, ExprCoreType.STRING)));
    LogicalPlan inputPlan = project(relation("query_range", prometheusMetricTable),
        finalProjectList, null);
    LogicalPlan optimizedPlan = prometheusMetricTable.optimize(
        inputPlan);
    assertEquals(inputPlan, optimizedPlan);
  }

}
