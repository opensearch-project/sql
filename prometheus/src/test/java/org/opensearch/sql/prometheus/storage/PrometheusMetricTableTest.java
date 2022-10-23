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
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
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

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

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
    expectedFieldTypes.put(LABELS, ExprCoreType.STRING);

    Map<String, ExprType> fieldTypes = prometheusMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verify(client, times(1)).getLabels(TestConstants.METRIC_NAME);
    assertNull(prometheusMetricTable.getPrometheusQueryRequest());
    assertNotNull(prometheusMetricTable.getMetricName());
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
    expectedFieldTypes.put(LABELS, ExprCoreType.STRING);

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
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(relation("query_range", prometheusMetricTable),
            finalProjectList, null));


    assertTrue(plan instanceof ProjectOperator);
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
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
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(relation("prometheus_http_requests_total", prometheusMetricTable),
            finalProjectList, null));


    assertTrue(plan instanceof ProjectOperator);
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusMetricScan prometheusMetricScan =
        (PrometheusMetricScan) ((ProjectOperator) plan).getInput();
    assertEquals("prometheus_http_requests_total", prometheusMetricScan.getRequest().getPromQl());
    assertEquals(3600 / 250 + "s", prometheusMetricScan.getRequest().getStep());
  }


  @Test
  void testImplementPrometheusQueries() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");

    //IndexScan with Filter
    PhysicalPlan plan = prometheusMetricTable.implement(
            indexScan("prometheus_http_total_requests",
            dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))))));

    assertTrue(plan instanceof PrometheusMetricScan);
    PrometheusQueryRequest prometheusQueryRequest = ((PrometheusMetricScan) plan).getRequest();
    assertEquals(3600 / 250 + "s", prometheusQueryRequest.getStep());
    assertEquals("prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"}",
        prometheusQueryRequest.getPromQl());


    // IndexScanAgg without Filter
    plan = prometheusMetricTable.implement(
        filter(
            indexScanAgg("prometheus_http_total_requests", ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("code", DSL.ref("code", STRING)))),
            dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))))));

    assertTrue(plan.getChild().get(0) instanceof PrometheusMetricScan);
    prometheusQueryRequest = ((PrometheusMetricScan) plan.getChild().get(0)).getRequest();
    assertEquals(
        "avg by(code) (prometheus_http_total_requests)",
        prometheusQueryRequest.getPromQl());

    // IndexScanAgg with Filter
    plan = prometheusMetricTable.implement(
        indexScanAgg("prometheus_http_total_requests",
            dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
            ImmutableList
                .of(DSL.named("AVG(@value)",
                    dsl.avg(DSL.ref("@value", INTEGER)))),
            ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))));
    assertTrue(plan instanceof PrometheusMetricScan);
    prometheusQueryRequest = ((PrometheusMetricScan) plan).getRequest();
    assertEquals(
        "avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        prometheusQueryRequest.getPromQl());


    // IndexScanAgg with Filter and Project
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals(request.getStep(), (3600 / 250) + "s");
    assertEquals(request.getPromQl(),
        "avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})");
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }


  @Test
  void testTimeRangeResolver() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    //Both endTime and startTime are set.
    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                          dsl.and(dsl.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                            DSL.literal(
                                fromObjectValue(dateFormat.format(new Date(startTime)),
                                    ExprCoreType.TIMESTAMP))),
                        dsl.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                          DSL.literal(
                            fromObjectValue(dateFormat.format(new Date(endTime)),
                                ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals((4800 / 250) + "s", request.getStep());
    assertEquals("avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }

  @Test
  void testTimeRangeResolverWithOutEndTimeInFilter() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    //Both endTime and startTime are set.
    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        dsl.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals((3600 / 250) + "s", request.getStep());
    assertEquals("avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }

  @Test
  void testTimeRangeResolverWithOutStartTimeInFilter() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    //Both endTime and startTime are set.
    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long endTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        dsl.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                            DSL.literal(
                                fromObjectValue(dateFormat.format(new Date(endTime)),
                                    ExprCoreType.TIMESTAMP))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals((3600 / 250) + "s", request.getStep());
    assertEquals("avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }




  @Test
  void testSpanResolverWithoutSpanExpression() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        dsl.and(dsl.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            dsl.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals((4800 / 250) + "s", request.getStep());
    assertEquals("avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }

  @Test
  void testSpanResolverWithSpanExpression() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        dsl.and(dsl.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            dsl.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)),
                    DSL.named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40", request.getStep());
    assertEquals("avg by(job) (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }

  @Test
  void testPrometheusQueryWithOnlySpanExpressionInGroupByList() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        dsl.and(dsl.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            dsl.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(
                    DSL.named("span", DSL.span(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                        DSL.literal(40), "")))),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals("40", request.getStep());
    assertEquals("avg  (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
  }

  @Test
  void testStatsWithNoGroupByList() {

    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");


    List<NamedExpression> finalProjectList = new ArrayList<>();
    Long endTime = new Date(System.currentTimeMillis()).getTime();
    Long startTime = new Date(System.currentTimeMillis() - 4800 * 1000).getTime();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    PhysicalPlan plan = prometheusMetricTable.implement(
        project(indexScanAgg("prometheus_http_total_requests",
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                    dsl.and(
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/"))),
                        dsl.and(dsl.gte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(startTime)),
                                        ExprCoreType.TIMESTAMP))),
                            dsl.lte(DSL.ref("@timestamp", ExprCoreType.TIMESTAMP),
                                DSL.literal(
                                    fromObjectValue(dateFormat.format(new Date(endTime)),
                                        ExprCoreType.TIMESTAMP)))))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of()),
            finalProjectList, null));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof PrometheusMetricScan);
    PrometheusQueryRequest request
        = ((PrometheusMetricScan) ((ProjectOperator) plan).getInput()).getRequest();
    assertEquals((4800 / 250) + "s", request.getStep());
    assertEquals("avg  (prometheus_http_total_requests{code=\"200\" , handler=\"/ready/\"})",
        request.getPromQl());
    List<NamedExpression> projectList = ((ProjectOperator) plan).getProjectList();
    List<String> outputFields
        = projectList.stream().map(NamedExpression::getName).collect(Collectors.toList());
    assertEquals(List.of(LABELS, TIMESTAMP, VALUE), outputFields);
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
                dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                        dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
                ImmutableList
                    .of(DSL.named("AVG(@value)",
                        dsl.avg(DSL.ref("@value", INTEGER))),
                        DSL.named("SUM(@value)",
                            dsl.avg(DSL.ref("@value", INTEGER)))),
                ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))));

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
        dsl.and(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
            dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))),
        ImmutableList
            .of(DSL.named("VAR_SAMP(@value)",
                    dsl.varSamp(DSL.ref("@value", INTEGER)))),
        ImmutableList.of(DSL.named("job", DSL.ref("job", STRING)))));

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> prometheusMetricTable.implement(plan));
    assertTrue(runtimeException.getMessage().contains("Prometheus Catalog only supports"));
  }

  @Test
  void testImplementWithORConditionInWhereClause() {
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, "prometheus_http_total_requests");
    LogicalPlan plan = indexScan("prometheus_http_total_requests",
            dsl.or(dsl.equal(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
                dsl.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))));
    RuntimeException exception
        = assertThrows(RuntimeException.class, () -> prometheusMetricTable.implement(plan));
    assertEquals("Prometheus Catalog doesn't support or in where command.", exception.getMessage());
  }

  @Test
  void testOptimize() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    PrometheusMetricTable prometheusMetricTable =
        new PrometheusMetricTable(client, prometheusQueryRequest);
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    LogicalPlan inputPlan = project(relation("query_range", prometheusMetricTable),
        finalProjectList, null);
    LogicalPlan optimizedPlan = prometheusMetricTable.optimize(
        inputPlan);
    assertEquals(inputPlan, optimizedPlan);
  }

}
