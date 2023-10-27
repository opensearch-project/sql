/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;

public abstract class StateModel implements ToXContentObject {
  public static final String VERSION_1_0 = "1.0";
  public static final String TYPE = "type";
  public static final String STATE = "state";
  public static final String LAST_UPDATE_TIME = "lastUpdateTime";

  public abstract String getId();

  public abstract long getSeqNo();

  public abstract long getPrimaryTerm();

  public interface CopyBuilder<T> {
    T of(T copy, long seqNo, long primaryTerm);
  }

  public interface StateCopyBuilder<T extends StateModel, S> {
    T of(T copy, S state, long seqNo, long primaryTerm);
  }

  public interface FromXContent<T extends StateModel> {
    T fromXContent(XContentParser parser, long seqNo, long primaryTerm);
  }
}
