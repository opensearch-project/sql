/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.linq4j.Enumerable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The customized table scan is implemented in OpenSearch module, to invoke this scan() method in
 * core module, we add this interface. Now the only implementation is CalciteEnumerableIndexScan.
 * When a RelNode after optimization is a Scannable, we can directly invoke scan() method to get the
 * result of the scan instead of codegen and compile via Linq4j expression.
 */
public interface Scannable {

  public Enumerable<@Nullable Object> scan();
}
