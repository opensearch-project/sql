/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.util.List;
import java.util.Objects;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.rest.RestStatus;

/**
 * Immutable owner identity used to authorize retained asynchronous query state.
 *
 * @param name authenticated principal, or {@code null} when no identity was supplied
 * @param requestedTenant requested security tenant
 * @param backendRoles backend roles captured when the job starts
 */
public record QueryJobOwner(String name, String requestedTenant, List<String> backendRoles) {

  /**
   * Identity used when OpenSearch Security does not provide caller information.
   *
   * <p>This identity does not bypass job ownership checks. A job owned by {@code UNSECURED} can be
   * accessed only by a caller represented by the same identity.
   */
  public static final QueryJobOwner UNSECURED = new QueryJobOwner(null, null, List.of());

  /**
   * Creates an immutable asynchronous query identity.
   *
   * @param name authenticated principal, or {@code null} when no identity was supplied
   * @param requestedTenant requested security tenant
   * @param backendRoles backend roles captured when the job starts
   * @throws IllegalArgumentException if {@code name} is blank
   */
  public QueryJobOwner {
    backendRoles = backendRoles == null ? List.of() : List.copyOf(backendRoles);
    if (name != null && name.isBlank()) {
      throw new IllegalArgumentException("PPL asynchronous query user must not be blank");
    }
  }

  /**
   * Captures the current caller from the OpenSearch thread context.
   *
   * @param threadContext current request thread context
   * @return immutable caller identity
   * @throws OpenSearchSecurityException if the security identity cannot be parsed
   */
  public static QueryJobOwner current(ThreadContext threadContext) {
    try {
      Object serialized =
          threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
      if (serialized == null) {
        return UNSECURED;
      }
      User user =
          serialized instanceof User currentUser
              ? currentUser
              : serialized instanceof String value ? User.parse(value) : null;
      if (user == null) {
        throw forbidden();
      }
      return new QueryJobOwner(user.getName(), user.getRequestedTenant(), user.getBackendRoles());
    } catch (RuntimeException e) {
      throw forbidden();
    }
  }

  /**
   * Verifies that a caller may access asynchronous query state owned by this identity.
   *
   * @param caller identity of the caller requesting access
   * @throws OpenSearchSecurityException if the caller does not match the owner identity
   */
  public void authorize(QueryJobOwner caller) {
    if (!Objects.equals(name, caller.name)
        || !Objects.equals(requestedTenant, caller.requestedTenant)
        || !caller.backendRoles.containsAll(backendRoles)) {
      throw forbidden();
    }
  }

  private static OpenSearchSecurityException forbidden() {
    return new OpenSearchSecurityException(
        "Not authorized to access PPL asynchronous query", RestStatus.FORBIDDEN);
  }
}
