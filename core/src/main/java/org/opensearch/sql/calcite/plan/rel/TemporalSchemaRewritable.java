/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Marker interface for OpenSearch RelNodes whose row type can be rewritten in place.
 *
 * <p>The {@link TemporalUdtRewriteShuttle} uses this interface to convert a scan's schema from
 * standard Calcite temporal types ({@code TIMESTAMP}, {@code DATE}, {@code TIME}) to the equivalent
 * UDT-backed types ({@code ExprTimeStampType}/{@code ExprDateType}/{@code ExprTimeType}, all
 * VARCHAR-backed at runtime) just before physical execution.
 *
 * <p>Wrapping a TableScan in a {@code LogicalProject(CAST...)} is not sufficient: Calcite's Linq4j
 * codegen reads the TableScan's row type to generate field accessors, and any standard temporal
 * type triggers {@code (Long) row[i]} casts at runtime, which fail when the runtime actually
 * delivers VARCHAR values via {@code OpenSearchExprValueFactory}. Implementations of this interface
 * must therefore return a NEW RelNode whose row type itself reflects the rewritten UDT schema (not
 * a wrapper that hides it from Calcite).
 */
public interface TemporalSchemaRewritable {
  /**
   * Return a copy of this RelNode whose row type is replaced by {@code rowType}. The structural
   * shape of the returned node should otherwise mirror {@code this}.
   */
  RelNode withRowType(RelDataType rowType);
}
