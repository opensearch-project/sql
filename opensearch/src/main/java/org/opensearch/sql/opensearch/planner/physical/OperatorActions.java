package org.opensearch.sql.opensearch.planner.physical;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.dataframe.ColumnMeta;
import org.opensearch.ml.common.dataframe.ColumnValue;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.ml.common.dataset.DataFrameInputDataset;
import org.opensearch.ml.common.parameter.FunctionName;
import org.opensearch.ml.common.parameter.MLAlgoParams;
import org.opensearch.ml.common.parameter.MLInput;
import org.opensearch.ml.common.parameter.MLPredictionOutput;
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

public abstract class OperatorActions extends PhysicalPlan {

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

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

  protected Map<String, ExprValue> convertRowIntoExprValue(ColumnMeta[] columnMetas, Row row) {
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    for (int i = 0; i < columnMetas.length; i++) {
      ColumnValue columnValue = row.getValue(i);
      String resultKeyName = columnMetas[i].getName();
      populateResultBuilder(columnValue, resultKeyName, resultBuilder);
    }
    return resultBuilder.build();
  }

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
        resultBuilder.put(resultKeyName, new ExprBooleanValue(columnValue.booleanValue()));
        break;
      default:
        break;
    }
  }

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

  protected ExprTupleValue buildResult(Iterator<Row> inputRowIter, DataFrame inputDataFrame,
                             MLPredictionOutput predictionResult, Iterator<Row> resultRowIter) {
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
