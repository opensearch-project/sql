/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.type;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;

/** Wrapper of spark data type */
@EqualsAndHashCode
@RequiredArgsConstructor
public class SparkDataType implements ExprType {

  /** Spark datatype name. */
  private final String typeName;

  @Override
  public String typeName() {
    return typeName;
  }
}
