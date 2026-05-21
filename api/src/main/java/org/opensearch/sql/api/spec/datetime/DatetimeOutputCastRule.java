/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping.isDatetimeType;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Wraps the root output with CAST(datetime → VARCHAR) for PPL wire-format compatibility.
 *
 * <p>Not a singleton: {@link RelHomogeneousShuttle} inherits a stateful {@code stack} field from
 * {@link org.apache.calcite.rel.RelShuttleImpl}, so a fresh instance must be used per plan().
 */
class DatetimeOutputCastRule extends RelHomogeneousShuttle {

  @Override
  public RelNode visit(RelNode other) {
    List<RelDataTypeField> fields = other.getRowType().getFieldList();
    if (fields.stream().noneMatch(f -> isDatetimeType(f.getType().getSqlTypeName()))) {
      return other;
    }

    RexBuilder rexBuilder = other.getCluster().getRexBuilder();
    List<RexNode> projects = new ArrayList<>(fields.size());
    List<String> names = new ArrayList<>(fields.size());

    // Cast datetime fields to VARCHAR for output; pass through others unchanged
    for (RelDataTypeField field : fields) {
      RexNode newField = rexBuilder.makeInputRef(other, field.getIndex());
      RelDataType fieldType = field.getType();
      if (isDatetimeType(fieldType.getSqlTypeName())) {
        projects.add(castToVarchar(rexBuilder, newField, fieldType));
      } else {
        projects.add(newField);
      }
      names.add(field.getName());
    }
    return LogicalProject.create(other, List.of(), projects, names);
  }

  private static RexNode castToVarchar(RexBuilder rexBuilder, RexNode expr, RelDataType fieldType) {
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelDataType varcharType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), fieldType.isNullable());
    return rexBuilder.makeCast(varcharType, expr);
  }
}
