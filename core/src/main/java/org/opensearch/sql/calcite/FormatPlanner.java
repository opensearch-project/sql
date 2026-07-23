/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.tree.Format;

/** Lowers the PPL {@code format} command to Calcite projects and a global aggregation. */
public class FormatPlanner {

  /** Builds a single-row relation containing the formatted search expression. */
  public RelNode plan(Format node, CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    if (node.getMaxResults() > 0) {
      builder.limit(0, node.getMaxResults());
    }

    List<RelDataTypeField> fields =
        builder.peek().getRowType().getFieldList().stream()
            .filter(field -> !field.getName().startsWith("_"))
            .sorted(Comparator.comparing(RelDataTypeField::getName))
            .toList();

    if (fields.isEmpty()) {
      builder.values(new String[] {"search"}, node.getEmptyString());
      return builder.peek();
    }

    List<RexNode> formattedFields =
        fields.stream().map(field -> formatField(field, node, context)).toList();
    RexNode joinedFields =
        arrayJoin(
            context, compactArray(context, formattedFields), " " + node.getColumnSeparator() + " ");
    RexNode rowExpression =
        concat(
            context,
            stringLiteral(node.getColumnPrefix() + " ", context),
            joinedFields,
            stringLiteral(" " + node.getColumnEnd(), context));
    RexNode nonEmptyRow = ifNotEmpty(joinedFields, rowExpression, context);

    builder.project(List.of(nonEmptyRow), List.of("__format_row"));
    RexNode rowRef = builder.field("__format_row");
    builder.aggregate(
        builder.groupKey(),
        builder
            .aggregateCall(SqlLibraryOperators.ARRAY_AGG, rowRef)
            .filter(builder.isNotNull(rowRef))
            .as("__format_rows"));

    RexNode joinedRows =
        arrayJoin(context, builder.field("__format_rows"), " " + node.getRowSeparator() + " ");
    RexNode formatted =
        concat(
            context,
            stringLiteral(node.getRowPrefix() + " ", context),
            joinedRows,
            stringLiteral(" " + node.getRowEnd(), context));
    RexNode result =
        context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            isNotEmpty(joinedRows, context),
            formatted,
            stringLiteral(node.getEmptyString(), context));
    builder.project(List.of(result), List.of("search"), true);
    return builder.peek();
  }

  private RexNode formatField(RelDataTypeField field, Format node, CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    RexNode value = builder.field(field.getIndex());
    RexNode formattedValue;
    RexNode hasValue = builder.isNotNull(value);
    String fieldName = formatFieldName(field.getName());
    boolean rawValue = field.getName().equals("search") || field.getName().equals("query");

    if (SqlTypeUtil.isArray(field.getType()) || SqlTypeUtil.isMultiset(field.getType())) {
      RelDataType varchar = context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
      RelDataType varcharArray = context.rexBuilder.getTypeFactory().createArrayType(varchar, -1);
      RexNode values =
          builder.call(
              SqlLibraryOperators.ARRAY_COMPACT, context.rexBuilder.makeCast(varcharArray, value));
      String repeatedValueSeparator =
          rawValue
              ? "\" " + node.getMvSeparator() + " \""
              : "\" " + node.getMvSeparator() + " " + fieldName + "=\"";
      RexNode joinedValues = arrayJoin(context, values, repeatedValueSeparator);
      formattedValue =
          concat(
              context,
              stringLiteral(rawValue ? "( \"" : "( " + fieldName + "=\"", context),
              joinedValues,
              stringLiteral("\" )", context));
      hasValue = builder.call(SqlStdOperatorTable.AND, hasValue, isNotEmpty(joinedValues, context));
    } else {
      RexNode stringValue = escapeValue(builder.cast(value, SqlTypeName.VARCHAR), context);
      formattedValue =
          concat(
              context,
              stringLiteral(rawValue ? "\"" : fieldName + "=\"", context),
              stringValue,
              stringLiteral("\"", context));
    }

    return builder.call(
        SqlStdOperatorTable.CASE,
        hasValue,
        formattedValue,
        context.rexBuilder.makeNullLiteral(formattedValue.getType()));
  }

  private RexNode escapeValue(RexNode value, CalcitePlanContext context) {
    RexNode escapedBackslashes =
        context.relBuilder.call(
            SqlStdOperatorTable.REPLACE,
            value,
            stringLiteral("\\", context),
            stringLiteral("\\\\", context));
    return context.relBuilder.call(
        SqlStdOperatorTable.REPLACE,
        escapedBackslashes,
        stringLiteral("\"", context),
        stringLiteral("\\\"", context));
  }

  private String formatFieldName(String fieldName) {
    if (fieldName.matches("[A-Za-z][A-Za-z0-9_]*")) {
      return fieldName;
    }
    return "\"" + fieldName.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private RexNode compactArray(CalcitePlanContext context, List<RexNode> values) {
    return context.relBuilder.call(
        SqlLibraryOperators.ARRAY_COMPACT,
        context.rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, values));
  }

  private RexNode arrayJoin(CalcitePlanContext context, RexNode values, String separator) {
    return context.relBuilder.call(
        SqlLibraryOperators.ARRAY_JOIN, values, stringLiteral(separator, context));
  }

  private RexNode ifNotEmpty(RexNode testedValue, RexNode result, CalcitePlanContext context) {
    return context.relBuilder.call(
        SqlStdOperatorTable.CASE,
        isNotEmpty(testedValue, context),
        result,
        context.rexBuilder.makeNullLiteral(result.getType()));
  }

  private RexNode isNotEmpty(RexNode value, CalcitePlanContext context) {
    return context.relBuilder.call(
        SqlStdOperatorTable.GREATER_THAN,
        context.relBuilder.call(SqlStdOperatorTable.CHAR_LENGTH, value),
        context.relBuilder.literal(0));
  }

  private RexNode concat(CalcitePlanContext context, RexNode... operands) {
    RexNode result = operands[0];
    for (int i = 1; i < operands.length; i++) {
      result = context.relBuilder.call(SqlStdOperatorTable.CONCAT, result, operands[i]);
    }
    return result;
  }

  private RexNode stringLiteral(String value, CalcitePlanContext context) {
    return context.rexBuilder.makeLiteral(
        value, context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
  }
}
