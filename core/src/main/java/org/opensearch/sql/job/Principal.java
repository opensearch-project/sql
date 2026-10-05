/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.List;
import java.util.Objects;

/**
 * Immutable caller identity retained with a query job for later authorization.
 *
 * <p>Principal is intentionally engine and framework neutral. It carries only what the lifecycle
 * layer needs to answer "is this caller allowed to view or cancel this job?". Capturing the caller
 * from a concrete runtime (OpenSearch ThreadContext, HTTP filter, RPC metadata, …) is the job of a
 * {@link SecurityAdapter} implementation.
 *
 * @param name authenticated principal, or {@code null} when the platform provides none
 * @param tenant requested security tenant, or {@code null} when tenants are not in use
 * @param backendRoles roles carried by the caller; defensively copied
 */
public record Principal(String name, String tenant, List<String> backendRoles) {

  /**
   * Identity used when no security plugin provides caller information.
   *
   * <p>{@code UNSECURED} does not bypass ownership checks: a job owned by {@code UNSECURED} is
   * accessible only through a request that also resolves to {@code UNSECURED}.
   */
  public static final Principal UNSECURED = new Principal(null, null, List.of());

  /**
   * Normalizes and validates the identity.
   *
   * <p>A {@code null} {@code backendRoles} list is normalized to an empty list; a caller may pass a
   * mutable list, which is defensively copied so the record cannot be mutated after construction. A
   * {@code null} {@code name} represents an unauthenticated caller (see {@link #UNSECURED});
   * anything else must be non-blank.
   *
   * @throws IllegalArgumentException if {@code name} is present but blank
   */
  public Principal {
    backendRoles = backendRoles == null ? List.of() : List.copyOf(backendRoles);
    if (name != null && name.isBlank()) {
      throw new IllegalArgumentException("Query job principal name must not be blank");
    }
  }

  /**
   * Returns {@code true} when {@code caller} is allowed to observe or mutate state owned by this
   * principal. A caller is allowed when the name and tenant match and the caller carries every
   * backend role the owner had.
   */
  public boolean allows(Principal caller) {
    Objects.requireNonNull(caller, "caller must not be null");
    return Objects.equals(name, caller.name)
        && Objects.equals(tenant, caller.tenant)
        && caller.backendRoles.containsAll(backendRoles);
  }
}
