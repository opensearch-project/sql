/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

/**
 * Bridge from the runtime security context to the neutral {@link Principal}.
 *
 * <p>Implementations live outside {@code core} and are provided by the hosting platform. The
 * production wiring is {@code OpenSearchSecurityAdapter} in the {@code opensearch} module; tests
 * inject a fixed principal.
 */
@FunctionalInterface
public interface SecurityAdapter {

  /** Adapter that reports every request as {@link Principal#UNSECURED}. */
  SecurityAdapter ALWAYS_UNSECURED = () -> Principal.UNSECURED;

  /** Returns the caller identity for the current request. */
  Principal current();
}
