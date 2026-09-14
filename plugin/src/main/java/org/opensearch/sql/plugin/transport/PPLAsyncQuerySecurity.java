/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.commons.authuser.User;
import org.opensearch.threadpool.ThreadPool;

/** Security-context helpers shared by PPL async transport actions. */
final class PPLAsyncQuerySecurity {
  private static final String SECURITY_USER_INFO_THREAD_CONTEXT = "_opendistro_security_user_info";

  private PPLAsyncQuerySecurity() {}

  static User currentUser(ThreadPool threadPool) {
    String serializedUser =
        threadPool.getThreadContext().getTransient(SECURITY_USER_INFO_THREAD_CONTEXT);
    return User.parse(serializedUser);
  }
}
