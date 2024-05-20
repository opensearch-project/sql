/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

public abstract class StateModel {
  public abstract String getId();

  public abstract long getSeqNo();

  public abstract long getPrimaryTerm();
}
