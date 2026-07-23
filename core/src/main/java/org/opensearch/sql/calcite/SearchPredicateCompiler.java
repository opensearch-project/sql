/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

/** Compiles runtime PPL search predicate text to an OpenSearch query_string expression. */
@FunctionalInterface
public interface SearchPredicateCompiler {

  String compile(String predicate);
}
