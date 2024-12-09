/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.ml.common.FunctionName.KMEANS;
import static org.opensearch.sql.utils.MLCommonsConstants.CENTROIDS;
import static org.opensearch.sql.utils.MLCommonsConstants.DISTANCE_TYPE;
import static org.opensearch.sql.utils.MLCommonsConstants.ITERATIONS;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.input.parameter.clustering.KMeansParams;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * ml-commons Physical operator to call machine learning interface to get results for algorithm
 * execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class MLCommonsOperator extends MLCommonsOperatorActions {
  @Getter private final PhysicalPlan input;

  @Getter private final String algorithm;

  @Getter private final Map<String, Literal> arguments;

  @Getter private final NodeClient nodeClient;

  @EqualsAndHashCode.Exclude private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    super.open();
    DataFrame inputDataFrame = generateInputDataset(input);
    MLAlgoParams mlAlgoParams = convertArgumentToMLParameter(arguments, algorithm);
    MLPredictionOutput predictionResult =
        getMLPredictionResult(
            FunctionName.valueOf(algorithm.toUpperCase()),
            mlAlgoParams,
            inputDataFrame,
            nodeClient);

    Iterator<Row> inputRowIter = inputDataFrame.iterator();
    Iterator<Row> resultRowIter = predictionResult.getPredictionResult().iterator();
    iterator =
        new Iterator<ExprValue>() {
          @Override
          public boolean hasNext() {
            return inputRowIter.hasNext();
          }

          @Override
          public ExprValue next() {
            return buildResult(inputRowIter, inputDataFrame, predictionResult, resultRowIter);
          }
        };
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitMLCommons(this, context);
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
    return List.of(input);
  }

  protected MLAlgoParams convertArgumentToMLParameter(
      Map<String, Literal> arguments, String algorithm) {
    switch (FunctionName.valueOf(algorithm.toUpperCase())) {
      case KMEANS:
        return KMeansParams.builder()
            .centroids(
                arguments.containsKey(CENTROIDS)
                    ? ((Integer) arguments.get(CENTROIDS).getValue())
                    : null)
            .iterations(
                arguments.containsKey(ITERATIONS)
                    ? ((Integer) arguments.get(ITERATIONS).getValue())
                    : null)
            .distanceType(
                arguments.containsKey(DISTANCE_TYPE)
                    ? (arguments.get(DISTANCE_TYPE).getValue() != null
                        ? KMeansParams.DistanceType.valueOf(
                            ((String) arguments.get(DISTANCE_TYPE).getValue()).toUpperCase())
                        : null)
                    : null)
            .build();
      default:
        // TODO: update available algorithms in the message when adding a new case
        throw new IllegalArgumentException(
            String.format(
                "unsupported algorithm: %s, available algorithms: %s.",
                FunctionName.valueOf(algorithm.toUpperCase()), KMEANS));
    }
  }
}
