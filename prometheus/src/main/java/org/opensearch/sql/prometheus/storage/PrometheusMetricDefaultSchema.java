/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

@Getter
@RequiredArgsConstructor
public enum PrometheusMetricDefaultSchema {
  DEFAULT_MAPPING(
      new ImmutableMap.Builder<String, ExprType>()
          .put(TIMESTAMP, ExprCoreType.TIMESTAMP)
          .put(VALUE, ExprCoreType.DOUBLE)
          .build());

  private final Map<String, ExprType> mapping;
}
