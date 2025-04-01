/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

/**
 * TO DO. support init with constant arguments https://github.com/opensearch-project/sql/issues/3490
 */
public interface UserDefinedFunction {
  Object eval(Object... args);
}
