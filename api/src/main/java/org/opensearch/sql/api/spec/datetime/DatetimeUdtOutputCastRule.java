/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.LanguageSpec.PostAnalysisRule;
import org.opensearch.sql.api.spec.datetime.DatetimeUdtExtension.UdtMapping;

/**
 * Wraps the plan with a projection that casts standard datetime output columns to VARCHAR so the
 * wire output matches PPL's String datetime contract. Runs after {@link DatetimeUdtNormalizeRule}
 * has converted UDT columns to their standard datetime types.
 */
public class DatetimeUdtOutputCastRule implements PostAnalysisRule {

  @Override
  public RelNode apply(RelNode plan) {
    List<RelDataTypeField> fields = plan.getRowType().getFieldList();
    boolean anyDatetime =
        fields.stream().anyMatch(f -> UdtMapping.fromStdType(f.getType()).isPresent());
    if (!anyDatetime) {
      return plan;
    }

    RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
    RelDataType varcharType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    List<RexNode> projects = new ArrayList<>(fields.size());
    List<String> names = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      RexNode ref = rexBuilder.makeInputRef(plan, field.getIndex());
      if (UdtMapping.fromStdType(field.getType()).isPresent()) {
        RelDataType nullableVarchar =
            rexBuilder
                .getTypeFactory()
                .createTypeWithNullability(varcharType, field.getType().isNullable());
        projects.add(rexBuilder.makeCast(nullableVarchar, ref));
      } else {
        projects.add(ref);
      }
      names.add(field.getName());
    }
    return LogicalProject.create(plan, List.of(), projects, names, Set.of());
  }
}
