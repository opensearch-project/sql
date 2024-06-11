/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

/** Interface for extension point to specify sessionId. Called when new session is created. */
public interface SessionIdProvider {
  String getSessionId(CreateSessionRequest createSessionRequest);
}
