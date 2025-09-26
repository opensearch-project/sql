/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.utils.MLCommonsConstants.CATEGORY_FIELD;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.transport.client.node.NodeClient;

/**
 * ml-commons Physical operator to call machine learning interface to get results for algorithm
 * execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class MLOperator extends MLCommonsOperatorActions {
  @Getter private final PhysicalPlan input;

  @Getter private final Map<String, Literal> arguments;

  @Getter private final NodeClient nodeClient;

  @EqualsAndHashCode.Exclude private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    super.open();
    Map<String, Object> args = processArgs(arguments);

    // Check if category_field is provided
    String categoryField =
        arguments.containsKey(CATEGORY_FIELD)
            ? (String) arguments.get(CATEGORY_FIELD).getValue()
            : null;

    // Only need to check train here, as action should be already checked in ml client.
    final boolean isPrediction = ((String) args.get("action")).equals("train") ? false : true;
    final Iterator<String> trainIter = Collections.singletonList("train").iterator();

    // For prediction mode, handle both categorized and non-categorized cases
    List<Pair<DataFrame, DataFrame>> inputDataFrames =
        generateCategorizedInputDataset(input, categoryField);
    List<MLOutput> mlOutputs =
        inputDataFrames.stream()
            .map(pair -> getMLOutput(pair.getRight(), args, nodeClient))
            .toList();
    Iterator<Pair<DataFrame, DataFrame>> inputDataFramesIter = inputDataFrames.iterator();
    Iterator<MLOutput> mlOutputIter = mlOutputs.iterator();

    iterator =
        new Iterator<>() {
          private DataFrame inputDataFrame = null;
          private Iterator<Row> inputRowIter = null;
          private MLOutput mlOutput = null;
          private Iterator<Row> resultRowIter = null;

          @Override
          public boolean hasNext() {
            if (isPrediction) {
              return (inputRowIter != null && inputRowIter.hasNext())
                  || inputDataFramesIter.hasNext();
            } else {
              boolean res = trainIter.hasNext();
              if (res) {
                trainIter.next();
              }
              return res;
            }
          }

          @Override
          public ExprValue next() {
            if (isPrediction) {
              if (inputRowIter == null || !inputRowIter.hasNext()) {
                Pair<DataFrame, DataFrame> pair = inputDataFramesIter.next();
                inputDataFrame = pair.getLeft();
                inputRowIter = inputDataFrame.iterator();
                mlOutput = mlOutputIter.next();
                resultRowIter = ((MLPredictionOutput) mlOutput).getPredictionResult().iterator();
              }
              return buildPPLResult(true, inputRowIter, inputDataFrame, mlOutput, resultRowIter);
            } else {
              // train case
              return buildPPLResult(false, null, null, mlOutputs.getFirst(), null);
            }
          }
        };
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitML(this, context);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  protected Map<String, Object> processArgs(Map<String, Literal> arguments) {
    Map<String, Object> res = new HashMap<>();
    arguments.forEach((k, v) -> res.put(k, v.getValue()));
    return res;
  }
}
