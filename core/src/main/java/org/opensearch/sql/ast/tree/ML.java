/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import static org.opensearch.sql.utils.MLCommonsConstants.ACTION;
import static org.opensearch.sql.utils.MLCommonsConstants.ALGO;
import static org.opensearch.sql.utils.MLCommonsConstants.ASYNC;
import static org.opensearch.sql.utils.MLCommonsConstants.CLUSTERID;
import static org.opensearch.sql.utils.MLCommonsConstants.KMEANS;
import static org.opensearch.sql.utils.MLCommonsConstants.MODELID;
import static org.opensearch.sql.utils.MLCommonsConstants.PREDICT;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALOUS;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALY_GRADE;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_SCORE;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_TIME_FIELD;
import static org.opensearch.sql.utils.MLCommonsConstants.STATUS;
import static org.opensearch.sql.utils.MLCommonsConstants.TASKID;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAIN;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAINANDPREDICT;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.analysis.TypeEnvironment;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.type.ExprCoreType;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
@RequiredArgsConstructor
@AllArgsConstructor
public class ML extends UnresolvedPlan {
  private UnresolvedPlan child;

  private final Map<String, Literal> arguments;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitML(this, context);
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  private String getAction() {
    return  (String) arguments.get(ACTION).getValue();
  }

  /**
   * Generate the ml output schema.
   *
   * @param env the current environment
   * @return the schema
   */
  public Map<String, ExprCoreType> getOutputSchema(TypeEnvironment env) {
    switch (getAction()) {
      case TRAIN:
        env.clearAllFields();
        return getTrainOutputSchema();
      case PREDICT:
      case TRAINANDPREDICT:
        return getPredictOutputSchema();
      default:
        throw new IllegalArgumentException(
                "Action error. Please indicate train, predict or trainandpredict.");
    }
  }

  /**
   * Generate the ml predict output schema.
   *
   * @return the schema
   */
  public Map<String, ExprCoreType> getPredictOutputSchema() {
    HashMap<String, ExprCoreType> res = new HashMap<>();
    String algo = arguments.containsKey(ALGO) ? (String) arguments.get(ALGO).getValue() : null;
    switch (algo) {
      case KMEANS:
        res.put(CLUSTERID, ExprCoreType.INTEGER);
        break;
      case RCF:
        res.put(RCF_SCORE, ExprCoreType.DOUBLE);
        if (arguments.containsKey(RCF_TIME_FIELD)) {
          res.put(RCF_ANOMALY_GRADE, ExprCoreType.DOUBLE);
          res.put((String) arguments.get(RCF_TIME_FIELD).getValue(), ExprCoreType.TIMESTAMP);
        } else {
          res.put(RCF_ANOMALOUS, ExprCoreType.BOOLEAN);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported algorithm: " + algo);
    }
    return res;
  }

  /**
   * Generate the ml train output schema.
   *
   * @return the schema
   */
  public Map<String, ExprCoreType> getTrainOutputSchema() {
    boolean isAsync = arguments.containsKey(ASYNC)
            ? (boolean) arguments.get(ASYNC).getValue() : false;
    Map<String, ExprCoreType> res = new HashMap<>(Map.of(STATUS, ExprCoreType.STRING));
    if (isAsync) {
      res.put(TASKID, ExprCoreType.STRING);
    } else {
      res.put(MODELID, ExprCoreType.STRING);
    }
    return res;
  }
}
