/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.DedupeOperator;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.FlattenOperator;
import org.opensearch.sql.planner.physical.LimitOperator;
import org.opensearch.sql.planner.physical.NestedOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.planner.physical.RareTopNOperator;
import org.opensearch.sql.planner.physical.RemoveOperator;
import org.opensearch.sql.planner.physical.RenameOperator;
import org.opensearch.sql.planner.physical.SortOperator;
import org.opensearch.sql.planner.physical.TakeOrderedOperator;
import org.opensearch.sql.planner.physical.TrendlineOperator;
import org.opensearch.sql.planner.physical.ValuesOperator;
import org.opensearch.sql.planner.physical.WindowOperator;
import org.opensearch.sql.storage.TableScanOperator;

/** Visitor that explains a physical plan to JSON format. */
public class Explain extends PhysicalPlanNodeVisitor<ExplainResponseNode, Object>
    implements Function<PhysicalPlan, ExplainResponse> {

  @Override
  public ExplainResponse apply(PhysicalPlan plan) {
    return new ExplainResponse(plan.accept(this, null));
  }

  @Override
  public ExplainResponseNode visitProject(ProjectOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of("fields", node.getProjectList().toString())));
  }

  @Override
  public ExplainResponseNode visitFilter(FilterOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of("conditions", node.getConditions().toString())));
  }

  @Override
  public ExplainResponseNode visitSort(SortOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of("sortList", describeSortList(node.getSortList()))));
  }

  @Override
  public ExplainResponseNode visitTakeOrdered(TakeOrderedOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of(
                    "limit", node.getLimit(),
                    "offset", node.getOffset(),
                    "sortList", describeSortList(node.getSortList()))));
  }

  @Override
  public ExplainResponseNode visitTableScan(TableScanOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode -> explainNode.setDescription(ImmutableMap.of("request", node.toString())));
  }

  @Override
  public ExplainResponseNode visitAggregation(AggregationOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of(
                    "aggregators", node.getAggregatorList().toString(),
                    "groupBy", node.getGroupByExprList().toString())));
  }

  @Override
  public ExplainResponseNode visitWindow(WindowOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of(
                    "function", node.getWindowFunction().toString(),
                    "definition",
                        ImmutableMap.of(
                            "partitionBy",
                                node.getWindowDefinition().getPartitionByList().toString(),
                            "sortList",
                                describeSortList(node.getWindowDefinition().getSortList())))));
  }

  @Override
  public ExplainResponseNode visitRename(RenameOperator node, Object context) {
    Map<String, String> renameMappingDescription =
        node.getMapping().entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(ImmutableMap.of("mapping", renameMappingDescription)));
  }

  @Override
  public ExplainResponseNode visitRemove(RemoveOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of("removeList", node.getRemoveList().toString())));
  }

  @Override
  public ExplainResponseNode visitEval(EvalOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of("expressions", convertPairListToMap(node.getExpressionList()))));
  }

  @Override
  public ExplainResponseNode visitFlatten(FlattenOperator node, Object context) {
    return explain(
            node,
            context,
            explainNode ->
                    explainNode.setDescription(
                            ImmutableMap.of("flattenField", node.getField())));
  }

  @Override
  public ExplainResponseNode visitDedupe(DedupeOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of(
                    "dedupeList", node.getDedupeList().toString(),
                    "allowedDuplication", node.getAllowedDuplication(),
                    "keepEmpty", node.getKeepEmpty(),
                    "consecutive", node.getConsecutive())));
  }

  @Override
  public ExplainResponseNode visitRareTopN(RareTopNOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of(
                    "commandType", node.getCommandType(),
                    "noOfResults", node.getNoOfResults(),
                    "fields", node.getFieldExprList().toString(),
                    "groupBy", node.getGroupByExprList().toString())));
  }

  @Override
  public ExplainResponseNode visitValues(ValuesOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode -> explainNode.setDescription(ImmutableMap.of("values", node.getValues())));
  }

  @Override
  public ExplainResponseNode visitLimit(LimitOperator node, Object context) {
    return explain(
        node,
        context,
        explanNode ->
            explanNode.setDescription(
                ImmutableMap.of("limit", node.getLimit(), "offset", node.getOffset())));
  }

  @Override
  public ExplainResponseNode visitNested(NestedOperator node, Object context) {
    return explain(
        node,
        context,
        explanNode -> explanNode.setDescription(ImmutableMap.of("nested", node.getFields())));
  }

  @Override
  public ExplainResponseNode visitTrendline(TrendlineOperator node, Object context) {
    return explain(
        node,
        context,
        explainNode ->
            explainNode.setDescription(
                ImmutableMap.of(
                    "computations",
                    describeTrendlineComputations(
                        node.getComputations().stream()
                            .map(Pair::getKey)
                            .collect(Collectors.toList())))));
  }

  protected ExplainResponseNode explain(
      PhysicalPlan node, Object context, Consumer<ExplainResponseNode> doExplain) {
    ExplainResponseNode explainNode = new ExplainResponseNode(getOperatorName(node));

    List<ExplainResponseNode> children = new ArrayList<>();
    for (PhysicalPlan child : node.getChild()) {
      children.add(child.accept(this, context));
    }
    explainNode.setChildren(children);

    doExplain.accept(explainNode);
    return explainNode;
  }

  private String getOperatorName(PhysicalPlan node) {
    return node.getClass().getSimpleName();
  }

  private <T, U> Map<String, String> convertPairListToMap(List<Pair<T, U>> pairs) {
    return pairs.stream()
        .collect(Collectors.toMap(p -> p.getLeft().toString(), p -> p.getRight().toString()));
  }

  private Map<String, Map<String, String>> describeSortList(
      List<Pair<Sort.SortOption, Expression>> sortList) {
    return sortList.stream()
        .collect(
            Collectors.toMap(
                p -> p.getRight().toString(),
                p ->
                    ImmutableMap.of(
                        "sortOrder", p.getLeft().getSortOrder().toString(),
                        "nullOrder", p.getLeft().getNullOrder().toString())));
  }

  private List<Map<String, String>> describeTrendlineComputations(
      List<Trendline.TrendlineComputation> computations) {
    return computations.stream()
        .map(
            computation ->
                ImmutableMap.of(
                    "computationType",
                        computation.getComputationType().name().toLowerCase(Locale.ROOT),
                    "numberOfDataPoints", computation.getNumberOfDataPoints().toString(),
                    "dataField", computation.getDataField().getChild().get(0).toString(),
                    "alias", computation.getAlias()))
        .collect(Collectors.toList());
  }
}
