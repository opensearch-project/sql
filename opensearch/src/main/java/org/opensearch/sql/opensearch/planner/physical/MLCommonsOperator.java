/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.ml.common.parameter.FunctionName.KMEANS;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.dataframe.ColumnMeta;
import org.opensearch.ml.common.dataframe.ColumnValue;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.dataset.DataFrameInputDataset;
import org.opensearch.ml.common.parameter.FunctionName;
import org.opensearch.ml.common.parameter.KMeansParams;
import org.opensearch.ml.common.parameter.MLAlgoParams;
import org.opensearch.ml.common.parameter.MLInput;
import org.opensearch.ml.common.parameter.MLPredictionOutput;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.MLClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * ml-commons Physical operator to call machine learning interface to get results for
 * algorithm execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class MLCommonsOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;

  @Getter
  private final String algorithm;

  @Getter
  private final List<Argument> arguments;

  @Getter
  private final NodeClient nodeClient;

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    super.open();
    DataFrame inputDataFrame = generateInputDataset();
    MLAlgoParams mlAlgoParams = convertArgumentToMLParameter(arguments.get(0), algorithm);
    MLInput mlinput = MLInput.builder()
            .algorithm(FunctionName.valueOf(algorithm.toUpperCase()))
            .parameters(mlAlgoParams)
            .inputDataset(new DataFrameInputDataset(inputDataFrame))
            .build();

    MachineLearningNodeClient machineLearningClient =
            MLClient.getMLClient(nodeClient);
    MLPredictionOutput predictionResult = (MLPredictionOutput) machineLearningClient
            .trainAndPredict(mlinput)
            .actionGet(30, TimeUnit.SECONDS);
    Iterator<Row> inputRowIter = inputDataFrame.iterator();
    Iterator<Row> resultRowIter = predictionResult.getPredictionResult().iterator();
    iterator = new Iterator<ExprValue>() {
      @Override
      public boolean hasNext() {
        return inputRowIter.hasNext();
      }

      @Override
      public ExprValue next() {
        ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
        resultBuilder.putAll(convertRowIntoExprValue(inputDataFrame.columnMetas(),
                inputRowIter.next()));
        resultBuilder.putAll(convertRowIntoExprValue(
                predictionResult.getPredictionResult().columnMetas(),
                resultRowIter.next()));
        return ExprTupleValue.fromExprValueMap(resultBuilder.build());
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
    return Collections.singletonList(input);
  }

  protected MLAlgoParams convertArgumentToMLParameter(Argument argument, String algorithm) {
    switch (FunctionName.valueOf(algorithm.toUpperCase())) {
      case KMEANS:
        if (argument.getValue().getValue() instanceof Number) {
          return KMeansParams.builder().centroids((Integer) argument.getValue().getValue()).build();
        } else {
          throw new IllegalArgumentException("unsupported Kmeans argument type:"
                  + argument.getValue().getType());
        }
      default:
        // TODO: update available algorithms in the message when adding a new case
        throw new IllegalArgumentException(
                String.format("unsupported algorithm: %s, available algorithms: %s.",
                FunctionName.valueOf(algorithm.toUpperCase()), KMEANS));
    }
  }

  private Map<String, ExprValue> convertRowIntoExprValue(ColumnMeta[] columnMetas, Row row) {
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    for (int i = 0; i < columnMetas.length; i++) {
      ColumnValue columnValue = row.getValue(i);
      String resultKeyName = columnMetas[i].getName();
      switch (columnValue.columnType()) {
        case INTEGER:
          resultBuilder.put(resultKeyName, new ExprIntegerValue(columnValue.intValue()));
          break;
        case DOUBLE:
          resultBuilder.put(resultKeyName, new ExprDoubleValue(columnValue.doubleValue()));
          break;
        case STRING:
          resultBuilder.put(resultKeyName, new ExprStringValue(columnValue.stringValue()));
          break;
        case SHORT:
          resultBuilder.put(resultKeyName, new ExprShortValue(columnValue.shortValue()));
          break;
        case LONG:
          resultBuilder.put(resultKeyName, new ExprLongValue(columnValue.longValue()));
          break;
        case FLOAT:
          resultBuilder.put(resultKeyName, new ExprFloatValue(columnValue.floatValue()));
          break;
        default:
          break;
      }
    }
    return resultBuilder.build();
  }

  private DataFrame generateInputDataset() {
    List<Map<String, Object>> inputData = new LinkedList<>();
    while (input.hasNext()) {
      inputData.add(new HashMap<String, Object>() {
        {
          input.next().tupleValue().forEach((key, value) -> put(key, value.value()));
        }
      });
    }

    return DataFrameBuilder.load(inputData);
  }
}

