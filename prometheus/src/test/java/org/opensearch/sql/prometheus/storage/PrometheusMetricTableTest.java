/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;
import static org.opensearch.sql.prometheus.utils.LogicalPlanUtils.indexScan;
import static org.opensearch.sql.prometheus.utils.LogicalPlanUtils.indexScanAgg;
import static org.opensearch.sql.prometheus.utils.LogicalPlanUtils.testLogicalPlanNode;

import com.google.common.collect.ImmutableList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.constants.TestConstants;
import org.opensearch.sql.prometheus.functions.scan.QueryRangeFunctionTableScanBuilder;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.storage.read.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
class PrometheusMetricTableTest {

  @Mock
  private PrometheusClient client;

  @Test
  @SneakyThrows
  void testGetFieldTypesFromMetric() {
    when(client.getLabels(TestConstants.METRIC_NAME)).thenReturn(List.of("label1", "label2"));
    PrometheusMetricTable prometheusMetricTable
        = new PrometheusMetricTable(client, TestConstants.METRIC_NAME);
    Map<String, ExprType> expectedFieldTypes = new HashMap<>();
    expectedFieldTypes.put("label1", ExprCoreType.STRING);
    expectedFieldTypes.put("label2", ExprCoreType.STRING);
    expectedFieldTypes.put(VALUE, ExprCoreType.DOUBLE);
    expectedFieldTypes.put(TIMESTAMP, ExprCoreType.TIMESTAMP);

    Map<String, ExprType> fieldTypes = prometheusMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verify(client, times(1)).getLabels(TestConstants.METRIC_NAME);
    assertNull(prometheusMetricTable.getPrometheusQueryRequest());
    assertNotNull(prometheusMetricTable.getMetricName());

    //testing Caching
    fieldTypes = prometheusMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verifyNoMoreInteractions(client);
    assertNull(prometheusMetricTable.getPrometheusQueryRequest());
    assertNotNull(prometheusMetricTable.getMetricName());
  }

  @Test
  @SneakyThrows
  void testGetFieldTypesFromPrometheusQueryRequest() {
    PrometheusMetricTable prometheusMetricTable
        = new PrometheusMetricTable(client, new PrometheusQueryRequest());
    Map<String, ExprType> expectedFieldTypes = new HashMap<>();
    expectedFieldTypes.put(VALUE, ExprCoreType.DOUBLE);
    expectedFieldTypes.put(TIMESTAMP, ExprCoreType.TIMESTAMP);
    expectedFieldTypes.put(LABELS, STRING);

    Map<String, ExprType> fieldTypes = prometheusMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verifyNoMoreInteractions(client);
    assertNotNull(prometheusMetricTable.getPrometheusQueryRequest());
    assertNull(prometheusMetricTable.getMetricName());
  }

  @Test
  void testImplementWithQueryRangeFunction() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl("test");
    prometheusQueryRequest.setStep("15m");
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(relation("query_range", prometheusMetricTable),
            finalProjectList, null));


    assertTrue(plan instanceof ProjectOperator);
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusMetricScan prometheusMetricScan =
        (PrometheusMetricScan) ((ProjectOperator) plan).getInput();
    assertEquals(prometheusQueryRequest, prometheusMetricScan.getRequest());
  }

  @Test
  void testImplementWithBasicMetricQuery() {
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_requests_total");
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(named("@value", ref("@value", ExprCoreType.DOUBLE)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(relation("prometheus_http_requests_total", prometheusMetricTable),
            finalProjectList, null));

    assertTrue(plan instanceof ProjectOperator);
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE), outputFields);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusMetricScan prometheusMetricScan =
        (PrometheusMetricScan) ((ProjectOperator) plan).getInput();
    assertEquals("prometheus_http_requests_total", prometheusMetricScan.getRequest().getPromQl());
    assertEquals(3600 / 250 + "s", prometheusMetricScan.getRequest().getStep());
  }


  @Test
  void testImplementPrometheusQueryWithStatsQueryAndNoFilter() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");

    // IndexScanAgg without Filter
    PhysicalPlan plan = prometheusMetricTable.implement(
        filter(
            indexScanAgg("prometheus_http_total_requests", ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("code", DSL.ref("code", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                    DSL.literal(40), "s")))),
            DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))))));

    assertTrue(plan.getChild().get(0) instanceof PrometheusMetricScan);
    PrometheusQueryRequest prometheusQueryRequest =
        ((PrometheusMetricScan) plan.getChild().get(0)).getRequest();
    assertEquals(
        "avg by(code) (avg_over_time(prometheus_http_total_requests[40s]))",
        prometheusQueryRequest.getPromQl());
  }

  @Test
  void testImplementPrometheusQueryWithStatsQueryAndFilter() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    // IndexScanAgg with Filter
    PhysicalPlan plan = prometheusMetricTable.implement(
        indexScanAgg("prometheus_http_total_requests",
            DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
            ImmutableList
                .of(named("AVG(@value)",
                    DSL.avg(DSL.ref("@value", INTEGER)))),
            ImmutableList.of(named("job", DSL.ref("job", STRING)),
                named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                    DSL.literal(40), "s")))));
    assertTrue(plan instanceof PrometheusMetricScan);
    PrometheusQueryRequest prometheusQueryRequest = ((PrometheusMetricScan) plan).getRequest();
    assertEquals(
        "avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        prometheusQueryRequest.getPromQl());

  }


  @Test
  void testImplementPrometheusQueryWithStatsQueryAndFilterAndProject() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");

    // IndexScanAgg with Filter and Project
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals(request.getStep(), "40s");
    assertEquals("avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }


  @Test
  void testTimeRangeResolver() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    //Both endTime and startTime are set.
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("job", DSL.ref("job", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40s", request.getStep());
    assertEquals("avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testTimeRangeResolverWithOutEndTimeInFilter() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    //Only endTime is set.
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                            DSL.literal(
                                fromObjectValue(dateFormat.format(new Date(startTime)),
                                    ExprCoreType.TIMESTAMP))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("job", DSL.ref("job", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40s", request.getStep());
    assertEquals("avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testTimeRangeResolverWithOutStartTimeInFilter() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    //Both endTime and startTime are set.
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long endTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                            DSL.literal(
                                fromObjectValue(dateFormat.format(new Date(endTime)),
                                    ExprCoreType.TIMESTAMP))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("job", DSL.ref("job", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40s", request.getStep());
    assertEquals("avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }


  @Test
  void testSpanResolverWithoutSpanExpression() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    LogicalPlan plan = project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                null),
            finalProjectList, null);
    RuntimeException runtimeException
        = Assertions.assertThrows(RuntimeException.class,
            () -> prometheusMetricTable.implement(plan));
    Assertions.assertEquals("Prometheus Catalog doesn't support "
            + "aggregations without span expression",
        runtimeException.getMessage());
  }

  @Test
  void testSpanResolverWithEmptyGroupByList() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    LogicalPlan plan = project(indexScanAgg("prometheus_http_total_requests",
            DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                DSL.and(
                    DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                    DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                            DSL.literal(
                                fromObjectValue(dateFormat.format(new Date(startTime)),
                                    ExprCoreType.TIMESTAMP))),
                        DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                            DSL.literal(
                                fromObjectValue(dateFormat.format(new Date(endTime)),
                                    ExprCoreType.TIMESTAMP)))))),
            ImmutableList
                .of(named("AVG(@value)",
                    DSL.avg(DSL.ref("@value", INTEGER)))),
            ImmutableList.of()),
        finalProjectList, null);
    RuntimeException runtimeException
        = Assertions.assertThrows(RuntimeException.class,
          () -> prometheusMetricTable.implement(plan));
    Assertions.assertEquals("Prometheus Catalog doesn't support "
            + "aggregations without span expression",
        runtimeException.getMessage());
  }

  @Test
  void testSpanResolverWithSpanExpression() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("job", DSL.ref("job", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40s", request.getStep());
    assertEquals("avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testExpressionWithMissingTimeUnitInSpanExpression() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    LogicalPlan logicalPlan = project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("job", DSL.ref("job", STRING)),
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "")))),
            finalProjectList, null);
    RuntimeException exception =
        Assertions.assertThrows(RuntimeException.class,
            () -> prometheusMetricTable.implement(logicalPlan));
    assertEquals("Missing TimeUnit in the span expression", exception.getMessage());
  }

  @Test
  void testPrometheusQueryWithOnlySpanExpressionInGroupByList() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(
                    named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40s", request.getStep());
    assertEquals("avg  (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testStatsWithNoGroupByList() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    DSL.and(
                        DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        DSL.and(DSL.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(named("AVG(@value)",
                        DSL.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(named("span",
                    DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                    DSL.literal(40), "s")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40s", request.getStep());
    assertEquals("avg  (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testImplementWithUnexpectedLogicalNode() {
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan plan = project(testLogicalPlanNode());
    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> prometheusMetricTable.implement(plan));
    assertEquals("unexpected plan node type class"
            + " org.opensearch.sql.prometheus.utils.LogicalPlanUtils$TestLogicalPlan",
        runtimeException.getMessage());
  }

  @Test
  void testMultipleAggregationsThrowsRuntimeException() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan plan = project(indexScanAgg("prometheus_http_total_requests",
        DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
            DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
        ImmutableList
            .of(named("AVG(@value)",
                    DSL.avg(DSL.ref("@value", INTEGER))),
                named("SUM(@value)",
                    DSL.avg(DSL.ref("@value", INTEGER)))),
        ImmutableList.of(named("job", DSL.ref("job", STRING)))));

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> prometheusMetricTable.implement(plan));
    assertEquals("Prometheus Catalog doesn't multiple aggregations in stats command",
        runtimeException.getMessage());
  }


  @Test
  void testUnSupportedAggregation() {
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan plan = project(indexScanAgg("prometheus_http_total_requests",
        DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
            DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
        ImmutableList
            .of(named("VAR_SAMP(@value)",
                DSL.varSamp(DSL.ref("@value", INTEGER)))),
        ImmutableList.of(named("job", DSL.ref("job", STRING)))));

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> prometheusMetricTable.implement(plan));
    assertTrue(runtimeException.getMessage().contains("Prometheus Catalog only supports"));
  }

  @Test
  void testImplementWithORConditionInWhereClause() {
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan plan = indexScan("prometheus_http_total_requests",
        DSL.or(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
            DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))));
    RuntimeException exception
        = assertThrows(RuntimeException.class, () -> prometheusMetricTable.implement(plan));
    assertEquals("Prometheus Datasource doesn't support or in where command.",
        exception.getMessage());
  }

  @Test
  void testImplementWithRelationAndFilter() {
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan logicalPlan = project(indexScan("prometheus_http_total_requests",
        DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
            DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))))),
        finalProjectList, null);
    PhysicalPlan physicalPlan = prometheusMetricTable.implement(logicalPlan);
    assertTrue(physicalPlan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) physicalPlan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) physicalPlan).getInput()).getRequest();
    assertEquals((3600 / 250) + "s", request.getStep());
    assertEquals("prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) physicalPlan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testImplementWithRelationAndTimestampFilter() {
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan logicalPlan = project(indexScan("prometheus_http_total_requests",
        DSL.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
            DSL.literal(
                fromObjectValue(dateFormat.format(new Date(endTime)),
                    ExprCoreType.TIMESTAMP)))
    ), finalProjectList, null);
    PhysicalPlan physicalPlan = prometheusMetricTable.implement(logicalPlan);
    assertTrue(physicalPlan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) physicalPlan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) physicalPlan).getInput()).getRequest();
    assertEquals((3600 / 250) + "s", request.getStep());
    assertEquals("prometheus_http_total_requests",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) physicalPlan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }


  @Test
  void testImplementWithRelationAndTimestampLTFilter() {
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan logicalPlan = project(indexScan("prometheus_http_total_requests",
        DSL.less(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
            DSL.literal(
                fromObjectValue(dateFormat.format(new Date(endTime)),
                    ExprCoreType.TIMESTAMP)))
    ), finalProjectList, null);
    PhysicalPlan physicalPlan = prometheusMetricTable.implement(logicalPlan);
    assertTrue(physicalPlan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) physicalPlan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) physicalPlan).getInput()).getRequest();
    assertEquals((3600 / 250) + "s", request.getStep());
    assertEquals("prometheus_http_total_requests",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) physicalPlan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }


  @Test
  void testImplementWithRelationAndTimestampGTFilter() {
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(DSL.named(VALUE, DSL.ref(VALUE, STRING)));
    finalProjectList.add(DSL.named(TIMESTAMP, DSL.ref(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan logicalPlan = project(indexScan("prometheus_http_total_requests",
        DSL.greater(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
            DSL.literal(
                fromObjectValue(dateFormat.format(new Date(endTime)),
                    ExprCoreType.TIMESTAMP)))
    ), finalProjectList, null);
    PhysicalPlan physicalPlan = prometheusMetricTable.implement(logicalPlan);
    assertTrue(physicalPlan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) physicalPlan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) physicalPlan).getInput()).getRequest();
    assertEquals((3600 / 250) + "s", request.getStep());
    assertEquals("prometheus_http_total_requests",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) physicalPlan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(VALUE, TIMESTAMP), outputFields);
  }

  @Test
  void testOptimize() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);
    List<NamedExpression> finalProjectList = new ArrayList<>();
    LogicalPlan inputPlan = project(relation("query_range", prometheusMetricTable),
        finalProjectList, null);
    LogicalPlan optimizedPlan = prometheusMetricTable.optimize(
        inputPlan);
    assertEquals(inputPlan, optimizedPlan);
  }

  @Test
  void testUnsupportedOperation() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);

    assertThrows(UnsupportedOperationException.class, prometheusMetricTable::exists);
    assertThrows(UnsupportedOperationException.class,
        () -> prometheusMetricTable.create(Collections.emptyMap()));
  }

  @Test
  void testImplementPrometheusQueryWithBackQuotedFieldNamesInStatsQuery() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    // IndexScanAgg with Filter
    PhysicalPlan plan = prometheusMetricTable.implement(
        indexScanAgg("prometheus_http_total_requests",
            DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
            ImmutableList
                .of(named("AVG(@value)",
                    DSL.avg(DSL.ref("@value", INTEGER)))),
            ImmutableList.of(named("`job`", DSL.ref("job", STRING)),
                named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                    DSL.literal(40), "s")))));
    assertTrue(plan instanceof PrometheusMetricScan);
    PrometheusQueryRequest prometheusQueryRequest = ((PrometheusMetricScan) plan).getRequest();
    assertEquals(
        "avg by(job) (avg_over_time"
            + "(prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}[40s]))",
        prometheusQueryRequest.getPromQl());

  }

  @Test
  void testImplementPrometheusQueryWithFilterQuery() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");

    // IndexScanAgg without Filter
    PhysicalPlan plan = prometheusMetricTable.implement(
            indexScan("prometheus_http_total_requests",
            DSL.and(DSL.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))))));

    assertTrue(plan instanceof PrometheusMetricScan);
    PrometheusQueryRequest prometheusQueryRequest =
        ((PrometheusMetricScan) plan).getRequest();
    assertEquals(
        "prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}",
        prometheusQueryRequest.getPromQl());
  }

  @Test
  void testImplementPrometheusQueryWithUnsupportedFilterQuery() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");

    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> prometheusMetricTable.implement(indexScan("prometheus_http_total_requests",
            DSL.and(DSL.lte(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))))));
    assertEquals("Prometheus Datasource doesn't support <= in where command.",
        exception.getMessage());
  }


  @Test
  void testCreateScanBuilderWithQueryRangeTableFunction() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl("test");
    prometheusQueryRequest.setStep("15m");
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);
    TableScanBuilder tableScanBuilder = prometheusMetricTable.createScanBuilder();
    Assertions.assertNotNull(tableScanBuilder);
    Assertions.assertTrue(tableScanBuilder instanceof QueryRangeFunctionTableScanBuilder);
  }

  @Test
  void testCreateScanBuilderWithPPLQuery() {
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, TestConstants.METRIC_NAME);
    TableScanBuilder tableScanBuilder = prometheusMetricTable.createScanBuilder();
    Assertions.assertNull(tableScanBuilder);
  }

}
