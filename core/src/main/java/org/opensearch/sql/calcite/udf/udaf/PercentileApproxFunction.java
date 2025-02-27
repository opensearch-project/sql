package org.opensearch.sql.calcite.udf.udaf;

import com.tdunning.math.stats.AVLTreeDigest;
import org.opensearch.sql.calcite.udf.Accumulator;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PercentileApproxFunction
    implements UserDefinedAggFunction<PercentileApproxFunction.PencentileApproAccumulator> {
  @Override
  public PencentileApproAccumulator init() {
    return new PencentileApproAccumulator();
  }

  // Add values to the accumulator
  @Override
  public PencentileApproAccumulator add(PencentileApproAccumulator acc, Object... values) {
    List<Object> allValues = Arrays.asList(values);
    Object targetValue = allValues.get(0);
    if (Objects.isNull(targetValue)) {
      return acc;
    }
    Number percentileValue = (Number) allValues.get(1);
    acc.add(((Number) targetValue).doubleValue(), percentileValue.intValue());
    return acc;
  }

  // Calculate the percentile
  @Override
  public Object result(PencentileApproAccumulator acc) {
    if (acc.size() == 0) {
      return null;
    }
    return acc.result();
  }

  public static class PencentileApproAccumulator extends AVLTreeDigest implements Accumulator {
    public static final double DEFAULT_COMPRESSION = 100.0;
    private double percent;

    public PencentileApproAccumulator() {
      super(DEFAULT_COMPRESSION);
      this.percent = 1.0;
    }

    public void add(float value, int percent) {
      this.percent = percent / 100.0;
      this.add(value);
    }

    public Object result() {
      return this.quantile(this.percent);
    }
  }
}
