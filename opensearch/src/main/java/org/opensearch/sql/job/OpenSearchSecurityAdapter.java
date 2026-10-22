/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Objects;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.rest.RestStatus;

/**
 * OpenSearch {@link SecurityAdapter} implementation.
 *
 * <p>Reads the caller identity from {@link
 * ConfigConstants#OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT} on the current transport thread's
 * {@link ThreadContext}. When the security plugin is absent (typical for local development) the
 * transient key is missing and {@link Principal#UNSECURED} is returned; jobs submitted under {@code
 * UNSECURED} are only observable by requests that also resolve to {@code UNSECURED}.
 *
 * <p>Kept in the {@code org.opensearch.sql.job} package to share visibility with the neutral {@link
 * Principal} record.
 */
public final class OpenSearchSecurityAdapter implements SecurityAdapter {

  private final ThreadContext threadContext;

  /**
   * @param threadContext transport-side thread context supplied by the plugin
   * @throws NullPointerException if {@code threadContext} is {@code null}
   */
  public OpenSearchSecurityAdapter(ThreadContext threadContext) {
    this.threadContext = Objects.requireNonNull(threadContext, "threadContext must not be null");
  }

  @Override
  public Principal current() {
    try {
      Object serialized =
          threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
      if (serialized == null) {
        return Principal.UNSECURED;
      }
      User user = toUser(serialized);
      if (user == null) {
        throw forbidden();
      }
      return new Principal(user.getName(), user.getRequestedTenant(), user.getBackendRoles());
    } catch (RuntimeException e) {
      throw forbidden();
    }
  }

  private static User toUser(Object serialized) {
    if (serialized instanceof User user) {
      return user;
    }
    if (serialized instanceof String encoded) {
      return User.parse(encoded);
    }
    return null;
  }

  private static OpenSearchSecurityException forbidden() {
    return new OpenSearchSecurityException(
        "Not authorized to access query job", RestStatus.FORBIDDEN);
  }
}
