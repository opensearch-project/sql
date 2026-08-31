/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.SearchPredicateCompiler;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.monitor.ResourceStatus;
import org.opensearch.sql.monitor.ResourceStatus.ResourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.DynamicQueryStringSpec;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;

class CalciteEnumerableIndexScanTest {

  @Test
  void runtimeQueryStringIsConjoinedWithExistingPushedFilter() {
    RelOptCluster cluster = mock(RelOptCluster.class);
    RelTraitSet traitSet = mock(RelTraitSet.class);
    RelOptTable table = mock(RelOptTable.class);
    RelDataType schema = mock(RelDataType.class);
    OpenSearchIndex osIndex = mock(OpenSearchIndex.class);
    OpenSearchClient client = mock(OpenSearchClient.class);
    OpenSearchResourceMonitor monitor = mock(OpenSearchResourceMonitor.class);
    OpenSearchRequest request = mock(OpenSearchRequest.class);
    OpenSearchRequestBuilder requestBuilder =
        new OpenSearchRequestBuilder(
            mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));

    when(schema.getFieldNames()).thenReturn(List.of("account_number"));
    when(osIndex.createRequestBuilder()).thenReturn(requestBuilder);
    when(osIndex.getClient()).thenReturn(client);
    when(client.getNodeClient()).thenReturn(Optional.empty());
    when(osIndex.getMaxResultWindow()).thenReturn(10000);
    when(osIndex.getQueryBucketSize()).thenReturn(1000);
    when(osIndex.buildRequest(same(requestBuilder))).thenReturn(request);
    when(osIndex.createOpenSearchResourceMonitor()).thenReturn(monitor);
    when(monitor.getStatus()).thenReturn(ResourceStatus.healthy(ResourceType.MEMORY));

    PushDownContext pushDownContext = new PushDownContext(osIndex);
    pushDownContext.add(
        PushDownType.FILTER,
        new FilterDigest(0, mock(RexNode.class)),
        (OSRequestBuilderAction) builder -> builder.pushDownFilterForCalcite(termQuery("age", 36)));

    RexNode runtimePart = mock(RexNode.class);
    RelDataType runtimePartType = mock(RelDataType.class);
    when(runtimePart.getType()).thenReturn(runtimePartType);
    when(runtimePartType.getSqlTypeName()).thenReturn(SqlTypeName.VARCHAR);
    SearchPredicateCompiler compiler = mock(SearchPredicateCompiler.class);
    when(compiler.compile("( account_number=\"6\" )")).thenReturn("account_number:6");
    pushDownContext.setDynamicQueryString(
        new DynamicQueryStringSpec(runtimePart, List.of(runtimePart), Set.of(0), compiler));

    CalciteEnumerableIndexScan scan =
        new CalciteEnumerableIndexScan(
            cluster, traitSet, List.of(), table, osIndex, schema, pushDownContext);
    Enumerator<Object> enumerator =
        scan.scan(new String[] {"( account_number=\"6\" )"}).enumerator();
    enumerator.close();

    assertEquals(
        boolQuery().filter(termQuery("age", 36)).filter(queryStringQuery("account_number:6")),
        requestBuilder.getSourceBuilder().query());
  }
}
