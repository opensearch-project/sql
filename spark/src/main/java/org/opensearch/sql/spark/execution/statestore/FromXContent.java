/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import org.opensearch.core.xcontent.XContentParser;

public interface FromXContent<T extends StateModel> {
  T fromXContent(XContentParser parser, long seqNo, long primaryTerm);
}
