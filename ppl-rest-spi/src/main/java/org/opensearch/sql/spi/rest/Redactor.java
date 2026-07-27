/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * Masks the value of a cell whose {@link Column} declares a given {@link RedactionClass}. A
 * platform (never a contributed endpoint plugin) registers one {@code Redactor} per class in the
 * sql side redaction registry; OSS registers none, so by default nothing is masked. Applied at the
 * single {@code rest} row-shaping choke point, once per string cell whose column class is not
 * {@link RedactionClass#NONE}.
 */
@FunctionalInterface
public interface Redactor {

  /**
   * Return the masked form of one cell value.
   *
   * @param value the coerced string value of the cell (never null when called from the choke point)
   * @return the value to emit in place of {@code value}
   */
  String mask(String value);
}
