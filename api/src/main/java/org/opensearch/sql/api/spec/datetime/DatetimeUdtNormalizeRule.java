/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.Optional;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping;

/**
 * Temporary patch that rewrites datetime UDT return types on RexCall nodes to standard Calcite
 * types.
 *
 * <p>Not a singleton: {@link RelHomogeneousShuttle} inherits a stateful {@code stack} field from
 * {@link org.apache.calcite.rel.RelShuttleImpl}, so a fresh instance must be used per plan().
 */
class DatetimeUdtNormalizeRule extends RelHomogeneousShuttle {

  @Override
  public RelNode visit(RelNode other) {
    RelNode visited = super.visit(other);
    RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    return visited.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            call = (RexCall) super.visitCall(call);
            Optional<UdtMapping> mapping = UdtMapping.fromUdtType(call.getType());
            if (mapping.isEmpty()) {
              return call;
            }

            // Normalize UDT return type to standard Calcite DATE/TIME/TIMESTAMP
            UdtMapping m = mapping.get();
            SqlTypeName stdTypeName = m.getStdType();
            RelDataType baseType =
                stdTypeName.allowsPrec()
                    ? typeFactory.createSqlType(
                        stdTypeName, typeFactory.getTypeSystem().getMaxPrecision(stdTypeName))
                    : typeFactory.createSqlType(stdTypeName);
            RelDataType stdType =
                typeFactory.createTypeWithNullability(baseType, call.getType().isNullable());
            return call.clone(stdType, call.getOperands());
          }
        });
  }
}
