/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request.system;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Prometheus system request query to get metadata Info.
 */
public interface PrometheusSystemRequest {

  /**
   * Search.
   *
   * @return list of ExprValue.
   */
  List<ExprValue> search();

}
