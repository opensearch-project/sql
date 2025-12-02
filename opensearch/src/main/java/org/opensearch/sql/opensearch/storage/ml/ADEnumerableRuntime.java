/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.ml;

import static org.opensearch.sql.opensearch.planner.physical.MLCommonsOperatorActions.getMLPredictionResult;
import static org.opensearch.sql.utils.MLCommonsConstants.ANOMALY_RATE;
import static org.opensearch.sql.utils.MLCommonsConstants.ANOMALY_SCORE_THRESHOLD;
import static org.opensearch.sql.utils.MLCommonsConstants.CATEGORY_FIELD;
import static org.opensearch.sql.utils.MLCommonsConstants.DATE_FORMAT;
import static org.opensearch.sql.utils.MLCommonsConstants.NUMBER_OF_TREES;
import static org.opensearch.sql.utils.MLCommonsConstants.OUTPUT_AFTER;
import static org.opensearch.sql.utils.MLCommonsConstants.SAMPLE_SIZE;
import static org.opensearch.sql.utils.MLCommonsConstants.SHINGLE_SIZE;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_DECAY;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_FIELD;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_ZONE;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAINING_DATA_SIZE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataframe.ColumnMeta;
import org.opensearch.ml.common.dataframe.ColumnValue;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.input.parameter.rcf.BatchRCFParams;
import org.opensearch.ml.common.input.parameter.rcf.FitRCFParams;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Enumerable AD operator runtime method to collect all rows of input enumerator, call ml-commons
 * node client API to train and predict ad result and then form new operator enumerator for
 * downstream operators.
 */
public class ADEnumerableRuntime {

  private ADEnumerableRuntime() {}

  public static Enumerable<Object[]> ad(
      final Enumerable<?> input,
      final String[] inputFieldNames,
      final String[] outputFieldNames,
      final Map<String, Object> arguments,
      final NodeClient nodeClient) {

    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        final String categoryField = (String) arguments.get(CATEGORY_FIELD);
        final Enumerator<?> inputEnumerator = input.enumerator();
        final Map<Object, List<Map<String, Object>>> groupedRows = new LinkedHashMap<>();
        while (inputEnumerator.moveNext()) {
          Object current = inputEnumerator.current();
          Object[] row = toRowArray(current);
          boolean hasNull = false;
          for (Object v : row) {
            if (v == null) {
              hasNull = true;
              break;
            }
          }
          if (hasNull) {
            continue;
          }

          Map<String, Object> rowMap = new LinkedHashMap<>();
          for (int i = 0; i < inputFieldNames.length; i++) {
            rowMap.put(inputFieldNames[i], row[i]);
          }

          Object categoryKey = null;
          if (categoryField != null) {
            categoryKey = rowMap.get(categoryField);
          }
          groupedRows.computeIfAbsent(categoryKey, k -> new ArrayList<>()).add(rowMap);
        }

        final List<Pair<DataFrame, DataFrame>> inputDataFrames = new ArrayList<>();
        for (List<Map<String, Object>> rows : groupedRows.values()) {
          if (rows.isEmpty()) {
            continue;
          }
          DataFrame fullDf = DataFrameBuilder.load(rows);
          List<Map<String, Object>> filteredRows =
              rows.stream()
                  .map(
                      r ->
                          r.entrySet().stream()
                              .filter(e -> !Objects.equals(e.getKey(), categoryField))
                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                  .collect(Collectors.toList());
          DataFrame featureDf = DataFrameBuilder.load(filteredRows);
          inputDataFrames.add(new ImmutablePair<>(fullDf, featureDf));
        }

        FunctionName rcfType;
        MLAlgoParams mlAlgoParams;
        boolean isBatch = !arguments.containsKey(TIME_FIELD);

        if (isBatch) {
          BatchRCFParams.BatchRCFParamsBuilder builder = BatchRCFParams.builder();

          Integer numberOfTrees = (Integer) arguments.get(NUMBER_OF_TREES);
          Integer sampleSize = (Integer) arguments.get(SAMPLE_SIZE);
          Integer outputAfter = (Integer) arguments.get(OUTPUT_AFTER);
          Integer trainingDataSize = (Integer) arguments.get(TRAINING_DATA_SIZE);
          Double anomalyScoreThreshold = (Double) arguments.get(ANOMALY_SCORE_THRESHOLD);

          builder
              .numberOfTrees(numberOfTrees)
              .sampleSize(sampleSize)
              .outputAfter(outputAfter)
              .trainingDataSize(trainingDataSize)
              .anomalyScoreThreshold(anomalyScoreThreshold);
          rcfType = FunctionName.BATCH_RCF;
          mlAlgoParams = builder.build();
        } else {
          FitRCFParams.FitRCFParamsBuilder builder = FitRCFParams.builder();

          Integer numberOfTrees = (Integer) arguments.get(NUMBER_OF_TREES);
          Integer shingleSize = (Integer) arguments.get(SHINGLE_SIZE);
          Integer sampleSize = (Integer) arguments.get(SAMPLE_SIZE);
          Integer outputAfter = (Integer) arguments.get(OUTPUT_AFTER);
          Double timeDecay = (Double) arguments.get(TIME_DECAY);
          Double anomalyRate = (Double) arguments.get(ANOMALY_RATE);
          String timeField = (String) arguments.get(TIME_FIELD);
          String dateFormat = (String) arguments.getOrDefault(DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
          String timeZone = (String) arguments.get(TIME_ZONE);

          builder
              .numberOfTrees(numberOfTrees)
              .shingleSize(shingleSize)
              .sampleSize(sampleSize)
              .outputAfter(outputAfter)
              .timeDecay(timeDecay)
              .anomalyRate(anomalyRate)
              .timeField(timeField)
              .dateFormat(dateFormat)
              .timeZone(timeZone);
          rcfType = FunctionName.FIT_RCF;
          mlAlgoParams = builder.build();
        }

        final List<MLPredictionOutput> predictionResults =
            inputDataFrames.stream()
                .map(
                    pair ->
                        getMLPredictionResult(rcfType, mlAlgoParams, pair.getRight(), nodeClient))
                .collect(Collectors.toList());

        final Iterator<Pair<DataFrame, DataFrame>> inputDataFramesIter = inputDataFrames.iterator();
        final Iterator<MLPredictionOutput> predictionResultIter = predictionResults.iterator();

        final Iterator<Object[]> resultIter =
            new Iterator<Object[]>() {
              private DataFrame inputDataFrame = null;
              private Iterator<Row> inputRowIter = null;
              private MLPredictionOutput predictionResult = null;
              private Iterator<Row> resultRowIter = null;

              @Override
              public boolean hasNext() {
                return (inputRowIter != null && inputRowIter.hasNext())
                    || inputDataFramesIter.hasNext();
              }

              @Override
              public Object[] next() {
                if (inputRowIter == null || !inputRowIter.hasNext()) {
                  Pair<DataFrame, DataFrame> pair = inputDataFramesIter.next();
                  inputDataFrame = pair.getLeft();
                  inputRowIter = inputDataFrame.iterator();
                  predictionResult = predictionResultIter.next();
                  resultRowIter = predictionResult.getPredictionResult().iterator();
                }

                Map<String, Object> rowMap =
                    buildRowMap(
                        inputDataFrame, inputRowIter,
                        predictionResult, resultRowIter);

                Object[] out = new Object[outputFieldNames.length];
                for (int i = 0; i < outputFieldNames.length; i++) {
                  out[i] = rowMap.get(outputFieldNames[i]);
                }
                return out;
              }
            };

        return new Enumerator<Object[]>() {
          private Object[] currentRow;

          @Override
          public Object[] current() {
            return currentRow;
          }

          @Override
          public boolean moveNext() {
            if (!resultIter.hasNext()) {
              return false;
            }
            currentRow = resultIter.next();
            return true;
          }

          @Override
          public void reset() {
            throw new UnsupportedOperationException(
                "reset is not supported for AD operators results");
          }

          @Override
          public void close() {
            // no-op
          }
        };
      }
    };
  }

  private static Map<String, Object> buildRowMap(
      DataFrame inputDataFrame,
      Iterator<Row> inputRowIter,
      MLPredictionOutput predictionResult,
      Iterator<Row> resultRowIter) {

    Row inputRow = inputRowIter.next();
    Map<String, Object> schemaMap = convertRowToObjectMap(inputDataFrame.columnMetas(), inputRow);

    Row resultRow = resultRowIter.next();
    Map<String, Object> resultMap =
        convertResultRowToObjectMap(
            predictionResult.getPredictionResult().columnMetas(), resultRow, schemaMap);

    // Output final Map = original data columns +  prediction result column
    Map<String, Object> out = new LinkedHashMap<>();
    out.putAll(resultMap);
    out.putAll(schemaMap);
    return out;
  }

  private static Map<String, Object> convertRowToObjectMap(ColumnMeta[] columnMetas, Row row) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < columnMetas.length; i++) {
      ColumnValue cv = row.getValue(i);
      String name = columnMetas[i].getName();
      map.put(name, toJavaValue(cv));
    }
    return map;
  }

  private static Map<String, Object> convertResultRowToObjectMap(
      ColumnMeta[] columnMetas, Row row, Map<String, Object> schema) {

    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < columnMetas.length; i++) {
      ColumnValue cv = row.getValue(i);
      String name = columnMetas[i].getName();
      if (schema.containsKey(name)) {
        name = name + "1";
      }
      map.put(name, toJavaValue(cv));
    }
    return map;
  }

  private static Object toJavaValue(ColumnValue columnValue) {
    switch (columnValue.columnType()) {
      case INTEGER:
        return columnValue.intValue();
      case DOUBLE:
        return columnValue.doubleValue();
      case STRING:
        return columnValue.stringValue();
      case SHORT:
        return columnValue.shortValue();
      case LONG:
        return columnValue.longValue();
      case FLOAT:
        return columnValue.floatValue();
      case BOOLEAN:
        return columnValue.booleanValue();
      default:
        return null;
    }
  }

  private static Object[] toRowArray(Object current) {
    if (current instanceof Object[]) {
      return (Object[]) current;
    }
    return new Object[] {current};
  }
}
