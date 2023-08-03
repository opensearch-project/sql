/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * ml-commons Physical operator to call machine learning interface to get results for
 * algorithm execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class MLOperator extends MLCommonsOperatorActions {
  @Getter
  private final PhysicalPlan input;

  @Getter
  private final Map<String, Literal> arguments;

  @Getter
  private final NodeClient nodeClient;

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    super.open();
    DataFrame inputDataFrame = generateInputDataset(input);
    Map<String, Object> args = processArgs(arguments);

    MLOutput mlOutput = getMLOutput(inputDataFrame, args, nodeClient);
    final Iterator<Row> inputRowIter = inputDataFrame.iterator();
    // Only need to check train here, as action should be already checked in ml client.
    final boolean isPrediction = ((String) args.get("action")).equals("train") ? false : true;
    //For train, only one row to return.
    final Iterator<String> trainIter = new ArrayList<String>() {
      {
        add("train");
      }
    }.iterator();
    final Iterator<Row> resultRowIter = isPrediction
            ? ((MLPredictionOutput) mlOutput).getPredictionResult().iterator()
            : null;
    iterator = new Iterator<ExprValue>() {
      @Override
      public boolean hasNext() {
        if (isPrediction) {
          return inputRowIter.hasNext();
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
        return buildPPLResult(isPrediction, inputRowIter, inputDataFrame, mlOutput, resultRowIter);
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
