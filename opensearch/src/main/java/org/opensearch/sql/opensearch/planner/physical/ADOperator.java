/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.utils.MLCommonsConstants.ANOMALY_RATE;
import static org.opensearch.sql.utils.MLCommonsConstants.ANOMALY_SCORE_THRESHOLD;
import static org.opensearch.sql.utils.MLCommonsConstants.DATE_FORMAT;
import static org.opensearch.sql.utils.MLCommonsConstants.NUMBER_OF_TREES;
import static org.opensearch.sql.utils.MLCommonsConstants.OUTPUT_AFTER;
import static org.opensearch.sql.utils.MLCommonsConstants.SAMPLE_SIZE;
import static org.opensearch.sql.utils.MLCommonsConstants.SHINGLE_SIZE;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_DECAY;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_FIELD;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_ZONE;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAINING_DATA_SIZE;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.parameter.BatchRCFParams;
import org.opensearch.ml.common.parameter.FitRCFParams;
import org.opensearch.ml.common.parameter.FunctionName;
import org.opensearch.ml.common.parameter.MLAlgoParams;
import org.opensearch.ml.common.parameter.MLPredictionOutput;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * AD Physical operator to call AD interface to get results for
 * algorithm execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ADOperator extends MLCommonsOperatorActions {

  @Getter
  private final PhysicalPlan input;

  @Getter
  private final Map<String, Literal> arguments;

  @Getter
  private final NodeClient nodeClient;

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  private FunctionName rcfType;

  @Override
  public void open() {
    super.open();
    DataFrame inputDataFrame = generateInputDataset(input);
    MLAlgoParams mlAlgoParams = convertArgumentToMLParameter(arguments);

    MLPredictionOutput predictionResult =
            getMLPredictionResult(rcfType, mlAlgoParams, inputDataFrame, nodeClient);

    Iterator<Row> inputRowIter = inputDataFrame.iterator();
    Iterator<Row> resultRowIter = predictionResult.getPredictionResult().iterator();
    iterator = new Iterator<ExprValue>() {
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
    return visitor.visitAD(this, context);
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

  protected MLAlgoParams convertArgumentToMLParameter(Map<String, Literal> arguments) {
    if (arguments.get(TIME_FIELD) == null) {
      rcfType = FunctionName.BATCH_RCF;
      return BatchRCFParams.builder()
              .numberOfTrees(arguments.containsKey(NUMBER_OF_TREES)
                      ? ((Integer) arguments.get(NUMBER_OF_TREES).getValue())
                      : null)
              .sampleSize(arguments.containsKey(SAMPLE_SIZE)
                      ? ((Integer) arguments.get(SAMPLE_SIZE).getValue())
                      : null)
              .outputAfter(arguments.containsKey(OUTPUT_AFTER)
                      ? ((Integer) arguments.get(OUTPUT_AFTER).getValue())
                      : null)
              .trainingDataSize(arguments.containsKey(TRAINING_DATA_SIZE)
                      ? ((Integer) arguments.get(TRAINING_DATA_SIZE).getValue())
                      : null)
              .anomalyScoreThreshold(arguments.containsKey(ANOMALY_SCORE_THRESHOLD)
                      ? ((Double) arguments.get(ANOMALY_SCORE_THRESHOLD).getValue())
                      : null)
              .build();
    }
    rcfType = FunctionName.FIT_RCF;
    return FitRCFParams.builder()
            .numberOfTrees(arguments.containsKey(NUMBER_OF_TREES)
                    ? ((Integer) arguments.get(NUMBER_OF_TREES).getValue())
                    : null)
            .shingleSize(arguments.containsKey(SHINGLE_SIZE)
                    ? ((Integer) arguments.get(SHINGLE_SIZE).getValue())
                    : null)
            .sampleSize(arguments.containsKey(SAMPLE_SIZE)
                    ? ((Integer) arguments.get(SAMPLE_SIZE).getValue())
                    : null)
            .outputAfter(arguments.containsKey(OUTPUT_AFTER)
                    ? ((Integer) arguments.get(OUTPUT_AFTER).getValue())
                    : null)
            .timeDecay(arguments.containsKey(TIME_DECAY)
                    ? ((Double) arguments.get(TIME_DECAY).getValue())
                    : null)
            .anomalyRate(arguments.containsKey(ANOMALY_RATE)
                    ? ((Double) arguments.get(ANOMALY_RATE).getValue())
                    : null)
            .timeField(arguments.containsKey(TIME_FIELD)
                    ? ((String) arguments.get(TIME_FIELD).getValue())
                    : null)
            .dateFormat(arguments.containsKey(DATE_FORMAT)
                    ? ((String) arguments.get(DATE_FORMAT).getValue())
                    : "yyyy-MM-dd HH:mm:ss")
            .timeZone(arguments.containsKey(TIME_ZONE)
                    ? ((String) arguments.get(TIME_ZONE).getValue())
                    : null)
            .build();
  }

}
