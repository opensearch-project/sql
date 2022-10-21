/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.physical;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataframe.ColumnMeta;
import org.opensearch.ml.common.dataframe.ColumnValue;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.dataset.DataFrameInputDataset;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.sql.data.model.ExprBooleanValue;
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

/**
 * Common method actions for ml-commons related operators.
 */
public abstract class MLCommonsOperatorActions extends PhysicalPlan {

  /**
   * generate ml-commons request input dataset.
   * @param input physical input
   * @return ml-commons dataframe
   */
  protected DataFrame generateInputDataset(PhysicalPlan input) {
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

  /**
   * Generate ml-commons request input dataset per each category based on a given category field.
   * Each category value will be a {@link DataFrame} pair, where the left one contains all fields
   * for building response, and the right one contains all fields except the aggregated field for
   * ml prediction. This is a temporary solution before ml-commons supports 2 dimensional input.
   *
   * @param input physical input
   * @param categoryField String, the field should be aggregated on
   * @return list of ml-commons dataframe pairs
   */
  protected List<Pair<DataFrame, DataFrame>> generateAggregatedInputDataset(PhysicalPlan input,
                                                                      String categoryField) {
    if (categoryField == null) {
      DataFrame dataFrame = generateInputDataset(input);
      return Collections.singletonList(new ImmutablePair<>(dataFrame, dataFrame));
    }

    Map<ExprValue, List<Map<String, Object>>> inputMap = new HashMap<>();
    while (input.hasNext()) {
      Map<String, ExprValue> tupleValue = input.next().tupleValue();
      ExprValue categoryValue = tupleValue.get(categoryField);
      List<Map<String, Object>> inputData =
          inputMap.computeIfAbsent(categoryValue, k -> new LinkedList<>());
      inputData.add(new HashMap<String, Object>() {
        {
          tupleValue.forEach((key, value) -> put(key, value.value()));
        }
      });
    }

    return inputMap.values().stream()
        .map(inputData -> {
          // categoryField should be excluded for ml-commons predictions
          List<Map<String, Object>> filteredInputData = inputData.stream().map(
                  row -> row.entrySet().stream().filter(entry -> !entry.getKey().equals(categoryField))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
              .collect(Collectors.toList());
          return new ImmutablePair<>(DataFrameBuilder.load(inputData),
              DataFrameBuilder.load(filteredInputData));
        }).collect(Collectors.toList());
  }

  /**
   * covert result schema into ExprValue.
   * @param columnMetas column metas
   * @param row row
   * @return a map of result schema in ExprValue format
   */
  protected Map<String, ExprValue> convertRowIntoExprValue(ColumnMeta[] columnMetas, Row row) {
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    for (int i = 0; i < columnMetas.length; i++) {
      ColumnValue columnValue = row.getValue(i);
      String resultKeyName = columnMetas[i].getName();
      populateResultBuilder(columnValue, resultKeyName, resultBuilder);
    }
    return resultBuilder.build();
  }

  /**
   * populate result map by ml-commons supported data type.
   * @param columnValue column value
   * @param resultKeyName result kay name
   * @param resultBuilder result builder
   */
  protected void populateResultBuilder(ColumnValue columnValue,
                                     String resultKeyName,
                                     ImmutableMap.Builder<String, ExprValue> resultBuilder) {
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
      case BOOLEAN:
        resultBuilder.put(resultKeyName, ExprBooleanValue.of(columnValue.booleanValue()));
        break;
      default:
        break;
    }
  }

  /**
   * concert result into ExprValue.
   * @param columnMetas column metas
   * @param row row
   * @param schema schema
   * @return a map of result in ExprValue format
   */
  protected Map<String, ExprValue> convertResultRowIntoExprValue(ColumnMeta[] columnMetas,
                                                               Row row,
                                                               Map<String, ExprValue> schema) {
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    for (int i = 0; i < columnMetas.length; i++) {
      ColumnValue columnValue = row.getValue(i);
      String resultKeyName = columnMetas[i].getName();
      // change key name to avoid duplicate key issue in result map
      // only value will be shown in the final returned result
      if (schema.containsKey(resultKeyName)) {
        resultKeyName = resultKeyName + "1";
      }
      populateResultBuilder(columnValue, resultKeyName, resultBuilder);

    }
    return resultBuilder.build();
  }

  /**
   * iterate result and built it into ExprTupleValue.
   * @param inputRowIter input row iterator
   * @param inputDataFrame input data frame
   * @param predictionResult prediction result
   * @param resultRowIter result row iterator
   * @return result in ExprTupleValue format
   */
  protected ExprTupleValue buildResult(Iterator<Row> inputRowIter,
                                       DataFrame inputDataFrame,
                                       MLPredictionOutput predictionResult,
                                       Iterator<Row> resultRowIter) {
    ImmutableMap.Builder<String, ExprValue> resultSchemaBuilder = new ImmutableMap.Builder<>();
    resultSchemaBuilder.putAll(convertRowIntoExprValue(inputDataFrame.columnMetas(),
            inputRowIter.next()));
    Map<String, ExprValue> resultSchema = resultSchemaBuilder.build();
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    resultBuilder.putAll(convertResultRowIntoExprValue(
            predictionResult.getPredictionResult().columnMetas(),
            resultRowIter.next(),
            resultSchema));
    resultBuilder.putAll(resultSchema);
    return ExprTupleValue.fromExprValueMap(resultBuilder.build());
  }

  /**
   * get ml-commons train and predict result.
   * @param functionName ml-commons algorithm name
   * @param mlAlgoParams ml-commons algorithm parameters
   * @param inputDataFrame input data frame
   * @param nodeClient node client
   * @return ml-commons train and predict result
   */
  protected MLPredictionOutput getMLPredictionResult(FunctionName functionName,
                                                     MLAlgoParams mlAlgoParams,
                                                     DataFrame inputDataFrame,
                                                     NodeClient nodeClient) {
    MLInput mlinput = MLInput.builder()
            .algorithm(functionName)
            .parameters(mlAlgoParams)
            .inputDataset(new DataFrameInputDataset(inputDataFrame))
            .build();

    MachineLearningNodeClient machineLearningClient =
            MLClient.getMLClient(nodeClient);

    return (MLPredictionOutput) machineLearningClient
            .trainAndPredict(mlinput)
            .actionGet(30, TimeUnit.SECONDS);
  }

}
