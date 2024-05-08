/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

public abstract class StateModel {
  public static final String VERSION_1_0 = "1.0";
  public static final String TYPE = "type";
  public static final String STATE = "state";
  public static final String LAST_UPDATE_TIME = "lastUpdateTime";

  public abstract String getId();

  public abstract long getSeqNo();

  public abstract long getPrimaryTerm();
}
