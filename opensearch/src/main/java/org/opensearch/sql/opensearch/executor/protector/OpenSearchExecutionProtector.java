/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.planner.physical.ADOperator;
import org.opensearch.sql.opensearch.planner.physical.MLCommonsOperator;
import org.opensearch.sql.opensearch.planner.physical.MLOperator;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.CursorCloseOperator;
import org.opensearch.sql.planner.physical.DedupeOperator;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.LimitOperator;
import org.opensearch.sql.planner.physical.LookupOperator;
import org.opensearch.sql.planner.physical.NestedOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.planner.physical.RareTopNOperator;
import org.opensearch.sql.planner.physical.RemoveOperator;
import org.opensearch.sql.planner.physical.RenameOperator;
import org.opensearch.sql.planner.physical.SortOperator;
import org.opensearch.sql.planner.physical.ValuesOperator;
import org.opensearch.sql.planner.physical.WindowOperator;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch Execution Protector. */
@RequiredArgsConstructor
@AllArgsConstructor
public class OpenSearchExecutionProtector extends ExecutionProtector {

  /** OpenSearch resource monitor. */
  private final ResourceMonitor resourceMonitor;

  @Nullable
  /** OpenSearch client. Maybe null * */
  private OpenSearchClient openSearchClient;

  public PhysicalPlan protect(PhysicalPlan physicalPlan) {
    return physicalPlan.accept(this, null);
  }

  /**
   * Don't protect {@link CursorCloseOperator} and entire nested tree, because {@link
   * CursorCloseOperator} as designed as no-op.
   */
  @Override
  public PhysicalPlan visitCursorClose(CursorCloseOperator node, Object context) {
    return node;
  }

  @Override
  public PhysicalPlan visitFilter(FilterOperator node, Object context) {
    return new FilterOperator(visitInput(node.getInput(), context), node.getConditions());
  }

  @Override
  public PhysicalPlan visitAggregation(AggregationOperator node, Object context) {
    return new AggregationOperator(
        visitInput(node.getInput(), context), node.getAggregatorList(), node.getGroupByExprList());
  }

  @Override
  public PhysicalPlan visitRareTopN(RareTopNOperator node, Object context) {
    return new RareTopNOperator(
        visitInput(node.getInput(), context),
        node.getCommandType(),
        node.getNoOfResults(),
        node.getFieldExprList(),
        node.getGroupByExprList());
  }

  @Override
  public PhysicalPlan visitRename(RenameOperator node, Object context) {
    return new RenameOperator(visitInput(node.getInput(), context), node.getMapping());
  }

  /** Decorate with {@link ResourceMonitorPlan}. */
  @Override
  public PhysicalPlan visitTableScan(TableScanOperator node, Object context) {
    return doProtect(node);
  }

  @Override
  public PhysicalPlan visitProject(ProjectOperator node, Object context) {
    return new ProjectOperator(
        visitInput(node.getInput(), context),
        node.getProjectList(),
        node.getNamedParseExpressions());
  }

  @Override
  public PhysicalPlan visitRemove(RemoveOperator node, Object context) {
    return new RemoveOperator(visitInput(node.getInput(), context), node.getRemoveList());
  }

  @Override
  public PhysicalPlan visitEval(EvalOperator node, Object context) {
    return new EvalOperator(visitInput(node.getInput(), context), node.getExpressionList());
  }

  @Override
  public PhysicalPlan visitNested(NestedOperator node, Object context) {
    return doProtect(
        new NestedOperator(
            visitInput(node.getInput(), context),
            node.getFields(),
            node.getGroupedPathsAndFields()));
  }

  @Override
  public PhysicalPlan visitDedupe(DedupeOperator node, Object context) {
    return new DedupeOperator(
        visitInput(node.getInput(), context),
        node.getDedupeList(),
        node.getAllowedDuplication(),
        node.getKeepEmpty(),
        node.getConsecutive());
  }

  @Override
  public PhysicalPlan visitLookup(LookupOperator node, Object context) {
    return new LookupOperator(
        visitInput(node.getInput(), context),
        node.getIndexName(),
        node.getMatchFieldMap(),
        node.getAppendOnly(),
        node.getCopyFieldMap(),
        lookup());
  }

  private BiFunction<String, Map<String, Object>, Map<String, Object>> lookup() {

    if (openSearchClient == null) {
      throw new RuntimeException(
          "Can not perform lookup because openSearchClient was null. This is likely a bug.");
    }

    return (indexName, inputMap) -> {
      Map<String, Object> matchMap = (Map<String, Object>) inputMap.get("_match");
      Set<String> copySet = (Set<String>) inputMap.get("_copy");

      BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

      for (Map.Entry<String, Object> f : matchMap.entrySet()) {
        BoolQueryBuilder orQueryBuilder = new BoolQueryBuilder();

        // Todo: Search with term and a match query? Or terms only?
        orQueryBuilder.should(new TermQueryBuilder(f.getKey(), f.getValue().toString()));
        orQueryBuilder.should(new MatchQueryBuilder(f.getKey(), f.getValue().toString()));
        orQueryBuilder.minimumShouldMatch(1);

        // filter is the same as "must" but ignores scoring
        boolQueryBuilder.filter(orQueryBuilder);
      }

      SearchResponse result =
          openSearchClient
              .getNodeClient()
              .search(
                  new SearchRequest(indexName)
                      .source(
                          SearchSourceBuilder.searchSource()
                              .fetchSource(
                                  copySet == null ? null : copySet.toArray(new String[0]), null)
                              .query(boolQueryBuilder)
                              .size(2)))
              .actionGet();

      int hits = result.getHits().getHits().length;

      if (hits == 0) {
        // null indicates no hits for the lookup found
        return null;
      }

      if (hits != 1) {
        throw new RuntimeException("too many hits for " + indexName + " (" + hits + ")");
      }

      return result.getHits().getHits()[0].getSourceAsMap();
    };
  }

  @Override
  public PhysicalPlan visitWindow(WindowOperator node, Object context) {
    return new WindowOperator(
        doProtect(visitInput(node.getInput(), context)),
        node.getWindowFunction(),
        node.getWindowDefinition());
  }

  /** Decorate with {@link ResourceMonitorPlan}. */
  @Override
  public PhysicalPlan visitSort(SortOperator node, Object context) {
    return doProtect(new SortOperator(visitInput(node.getInput(), context), node.getSortList()));
  }

  /**
   * Values are a sequence of rows of literal value in memory which doesn't need memory protection.
   */
  @Override
  public PhysicalPlan visitValues(ValuesOperator node, Object context) {
    return node;
  }

  @Override
  public PhysicalPlan visitLimit(LimitOperator node, Object context) {
    return new LimitOperator(
        visitInput(node.getInput(), context), node.getLimit(), node.getOffset());
  }

  @Override
  public PhysicalPlan visitMLCommons(PhysicalPlan node, Object context) {
    MLCommonsOperator mlCommonsOperator = (MLCommonsOperator) node;
    return doProtect(
        new MLCommonsOperator(
            visitInput(mlCommonsOperator.getInput(), context),
            mlCommonsOperator.getAlgorithm(),
            mlCommonsOperator.getArguments(),
            mlCommonsOperator.getNodeClient()));
  }

  @Override
  public PhysicalPlan visitAD(PhysicalPlan node, Object context) {
    ADOperator adOperator = (ADOperator) node;
    return doProtect(
        new ADOperator(
            visitInput(adOperator.getInput(), context),
            adOperator.getArguments(),
            adOperator.getNodeClient()));
  }

  @Override
  public PhysicalPlan visitML(PhysicalPlan node, Object context) {
    MLOperator mlOperator = (MLOperator) node;
    return doProtect(
        new MLOperator(
            visitInput(mlOperator.getInput(), context),
            mlOperator.getArguments(),
            mlOperator.getNodeClient()));
  }

  PhysicalPlan visitInput(PhysicalPlan node, Object context) {
    if (null == node) {
      return node;
    } else {
      return node.accept(this, context);
    }
  }

  protected PhysicalPlan doProtect(PhysicalPlan node) {
    if (isProtected(node)) {
      return node;
    }
    return new ResourceMonitorPlan(node, resourceMonitor);
  }

  private boolean isProtected(PhysicalPlan node) {
    return (node instanceof ResourceMonitorPlan);
  }
}
