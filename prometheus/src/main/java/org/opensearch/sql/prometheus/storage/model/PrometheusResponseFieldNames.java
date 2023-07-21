/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.model;

import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;


@Getter
@Setter
public class PrometheusResponseFieldNames {

  private String valueFieldName = VALUE;
  private ExprType valueType = DOUBLE;
  private String timestampFieldName = TIMESTAMP;
  private List<NamedExpression> groupByList;

}
