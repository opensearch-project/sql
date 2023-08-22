/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/** OpenSearch system request query against the system index. */
public interface OpenSearchSystemRequest {

  /**
   * Search.
   *
   * @return list of ExprValue.
   */
  List<ExprValue> search();
}
