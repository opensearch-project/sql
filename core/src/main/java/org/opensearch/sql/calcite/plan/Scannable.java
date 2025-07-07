/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.linq4j.Enumerable;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Scannable {

  public Enumerable<@Nullable Object> scan();
}
