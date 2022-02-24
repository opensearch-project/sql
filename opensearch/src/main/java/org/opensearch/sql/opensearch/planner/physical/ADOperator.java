package org.opensearch.sql.opensearch.planner.physical;

import com.google.common.collect.ImmutableMap;
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
import org.opensearch.ml.common.parameter.BatchRCFParams;
import org.opensearch.ml.common.parameter.FitRCFParams;
import org.opensearch.ml.common.parameter.FunctionName;
import org.opensearch.ml.common.parameter.KMeansParams;
import org.opensearch.ml.common.parameter.MLAlgoParams;
import org.opensearch.ml.common.parameter.MLInput;
import org.opensearch.ml.common.parameter.MLPredictionOutput;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Literal;
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
import sun.swing.AccumulativeRunnable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.ml.common.parameter.FunctionName.KMEANS;

/**
 * AD Physical operator to call AD interface to get results for
 * algorithm execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ADOperator extends PhysicalPlan {

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
    DataFrame inputDataFrame = generateInputDataset();
    MLAlgoParams mlAlgoParams = convertArgumentToMLParameter(arguments);

    MLInput mlinput = MLInput.builder()
            .algorithm(rcfType)
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
    return visitor.visitAD(this, context);
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    return null;
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return null;
  }

  protected MLAlgoParams convertArgumentToMLParameter(Map<String, Literal> arguments) {
    if (arguments.get("time_field").getValue() == null) {
      rcfType = FunctionName.BATCH_RCF;
      return BatchRCFParams.builder()
              .shingleSize((Integer) arguments.get("shingle_size").getValue())
              .build();
    }
    rcfType = FunctionName.FIT_RCF;
    return FitRCFParams.builder()
            .shingleSize((Integer) arguments.get("shingle_size").getValue())
            .timeDecay((Double) arguments.get("time_decay").getValue())
            .timeField((String) arguments.get("time_field").getValue())
            .build();
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
          input.next().tupleValue().forEach((key, value)
                  -> put(key, value.value()));
        }
      });
    }

    return DataFrameBuilder.load(inputData);
  }
}
