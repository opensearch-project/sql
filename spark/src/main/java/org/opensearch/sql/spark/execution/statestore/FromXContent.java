package org.opensearch.sql.spark.execution.statestore;

import org.opensearch.core.xcontent.XContentParser;

public interface FromXContent<T extends StateModel> {
  T fromXContent(XContentParser parser, long seqNo, long primaryTerm);
}
