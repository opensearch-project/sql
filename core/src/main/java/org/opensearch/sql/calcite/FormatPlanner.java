/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.tree.Format;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Lowers the PPL {@code format} command to Calcite projects and a global aggregation. */
public class FormatPlanner {

  private static final String SEARCH_FIELD = "search";
  private static final String RAW_SEARCH_FIELD = "__raw_search";
  private static final String RAW_SEARCH_COUNT_FIELD = "__raw_search_count";
  private static final String RAW_SEARCH_VALUE_FIELD = "__raw_search_value";
  private static final String FORMAT_ROW_FIELD = "__format_row";
  private static final String FORMAT_ROWS_FIELD = "__format_rows";
  private static final String FORMAT_ORDER_FIELD_PREFIX = "__format_order_";

  /** Builds a single-row relation containing the formatted search expression. */
  public RelNode plan(Format node, CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    if (node.getMaxResults() > 0) {
      builder.limit(0, node.getMaxResults());
    }

    List<RelDataTypeField> fields =
        builder.peek().getRowType().getFieldList().stream()
            .filter(field -> !METADATAFIELD_TYPE_MAP.containsKey(field.getName()))
            .sorted(Comparator.comparing(RelDataTypeField::getName))
            .toList();

    if (fields.isEmpty()) {
      builder.values(new String[] {SEARCH_FIELD}, node.getEmptyString());
      return builder.peek();
    }

    Optional<RelDataTypeField> scalarSearchField =
        fields.stream()
            .filter(field -> field.getName().equals(SEARCH_FIELD))
            .filter(
                field ->
                    !SqlTypeUtil.isArray(field.getType())
                        && !SqlTypeUtil.isMultiset(field.getType()))
            .findFirst();
    if (node.isImplicit() && scalarSearchField.isPresent()) {
      return planImplicitSearchField(node, context, fields, scalarSearchField.get());
    }

    List<RexNode> formattedFields =
        fields.stream().map(field -> formatField(field, node, context)).toList();
    RexNode nonEmptyRow = formatRow(formattedFields, node, context);

    aggregateRows(nonEmptyRow, context);
    builder.project(List.of(formatAggregatedRows(node, context)), List.of(SEARCH_FIELD), true);
    return builder.peek();
  }

  /**
   * At an implicit subsearch boundary, a scalar {@code search} field has special behavior. Only the
   * first row participates; a non-null {@code search} value is injected verbatim and suppresses the
   * row's other fields. If that value is null, the first row is formatted normally without the
   * {@code search} field.
   */
  private RelNode planImplicitSearchField(
      Format node,
      CalcitePlanContext context,
      List<RelDataTypeField> fields,
      RelDataTypeField searchField) {
    RelBuilder builder = context.relBuilder;
    builder.limit(0, 1);

    RexNode rawSearch = builder.cast(builder.field(searchField.getIndex()), SqlTypeName.VARCHAR);
    List<RexNode> fallbackFields =
        fields.stream()
            .filter(field -> field != searchField)
            .map(field -> formatField(field, node, context))
            .toList();
    RexNode fallbackRow = formatRow(fallbackFields, node, context);

    builder.project(List.of(rawSearch, fallbackRow), List.of(RAW_SEARCH_FIELD, FORMAT_ROW_FIELD));
    RexNode rawSearchRef = builder.field(RAW_SEARCH_FIELD);
    RexNode rowRef = builder.field(FORMAT_ROW_FIELD);
    builder.aggregate(
        builder.groupKey(),
        builder.aggregateCall(SqlStdOperatorTable.COUNT, rawSearchRef).as(RAW_SEARCH_COUNT_FIELD),
        builder.aggregateCall(SqlStdOperatorTable.MAX, rawSearchRef).as(RAW_SEARCH_VALUE_FIELD),
        builder.aggregateCall(SqlLibraryOperators.ARRAY_AGG, rowRef).as(FORMAT_ROWS_FIELD));

    RexNode hasRawSearch =
        builder.call(
            SqlStdOperatorTable.GREATER_THAN,
            builder.field(RAW_SEARCH_COUNT_FIELD),
            builder.literal(0));
    RexNode result =
        builder.call(
            SqlStdOperatorTable.CASE,
            hasRawSearch,
            builder.field(RAW_SEARCH_VALUE_FIELD),
            formatAggregatedRows(node, context));
    builder.project(List.of(result), List.of(SEARCH_FIELD), true);
    return builder.peek();
  }

  private RexNode formatRow(
      List<RexNode> formattedFields, Format node, CalcitePlanContext context) {
    if (formattedFields.isEmpty()) {
      RelDataType varchar = context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
      return context.rexBuilder.makeNullLiteral(varchar);
    }
    RexNode joinedFields =
        arrayJoin(
            context, compactArray(context, formattedFields), " " + node.getColumnSeparator() + " ");
    RexNode rowExpression =
        concat(
            context,
            stringLiteral(node.getColumnPrefix() + " ", context),
            joinedFields,
            stringLiteral(" " + node.getColumnEnd(), context));
    return ifNotEmpty(joinedFields, rowExpression, context);
  }

  private void aggregateRows(RexNode nonEmptyRow, CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    List<RelFieldCollation> ordering = inputOrdering(builder.peek());
    if (ordering.isEmpty()) {
      builder.project(List.of(nonEmptyRow), List.of(FORMAT_ROW_FIELD));
    } else {
      List<RexNode> projections = new ArrayList<>();
      projections.add(nonEmptyRow);
      projections.addAll(builder.fields());
      List<String> names = new ArrayList<>();
      names.add(FORMAT_ROW_FIELD);
      for (int i = 0; i < projections.size() - 1; i++) {
        names.add(FORMAT_ORDER_FIELD_PREFIX + i);
      }
      builder.project(projections, names, true);
    }
    RexNode rowRef = builder.field(FORMAT_ROW_FIELD);
    RelBuilder.AggCall rows = builder.aggregateCall(SqlLibraryOperators.ARRAY_AGG, rowRef);
    if (!ordering.isEmpty()) {
      rows = rows.sort(ordering.stream().map(order -> orderExpression(order, builder)).toList());
    }
    builder.aggregate(builder.groupKey(), rows.as(FORMAT_ROWS_FIELD));
  }

  private List<RelFieldCollation> inputOrdering(RelNode input) {
    return input.getCluster().getMetadataQuery().collations(input).stream()
        .map(RelCollation::getFieldCollations)
        .filter(ordering -> !ordering.isEmpty())
        .findFirst()
        .orElse(List.of());
  }

  private RexNode orderExpression(RelFieldCollation order, RelBuilder builder) {
    RexNode expression = builder.field(order.getFieldIndex() + 1);
    if (order.getDirection().isDescending()) {
      expression = builder.desc(expression);
    }
    if (order.nullDirection == RelFieldCollation.NullDirection.FIRST) {
      expression = builder.nullsFirst(expression);
    } else if (order.nullDirection == RelFieldCollation.NullDirection.LAST) {
      expression = builder.nullsLast(expression);
    }
    return expression;
  }

  private RexNode formatAggregatedRows(Format node, CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    RexNode nonNullRows =
        builder.call(SqlLibraryOperators.ARRAY_COMPACT, builder.field(FORMAT_ROWS_FIELD));
    RexNode joinedRows = arrayJoin(context, nonNullRows, " " + node.getRowSeparator() + " ");
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
    return result;
  }

  private RexNode formatField(RelDataTypeField field, Format node, CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    RexNode value = builder.field(field.getIndex());
    RexNode formattedValue;
    RexNode hasValue = builder.isNotNull(value);
    String fieldName = formatFieldName(field.getName());

    if (SqlTypeUtil.isArray(field.getType()) || SqlTypeUtil.isMultiset(field.getType())) {
      RelDataType varchar = context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
      RelDataType varcharArray = context.rexBuilder.getTypeFactory().createArrayType(varchar, -1);
      RexNode values =
          builder.call(
              SqlLibraryOperators.ARRAY_COMPACT, context.rexBuilder.makeCast(varcharArray, value));
      values = escapeArrayValues(values, varchar, context);
      String repeatedValueSeparator = "\" " + node.getMvSeparator() + " " + fieldName + "=\"";
      RexNode joinedValues = arrayJoin(context, values, repeatedValueSeparator);
      formattedValue =
          concat(
              context,
              stringLiteral("( " + fieldName + "=\"", context),
              joinedValues,
              stringLiteral("\" )", context));
      hasValue = builder.call(SqlStdOperatorTable.AND, hasValue, isNotEmpty(joinedValues, context));
    } else {
      RexNode stringValue = builder.cast(value, SqlTypeName.VARCHAR);
      formattedValue =
          concat(
              context,
              stringLiteral(fieldName + "=\"", context),
              escapeValue(stringValue, context),
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

  private RexNode escapeArrayValues(
      RexNode values, RelDataType varchar, CalcitePlanContext context) {
    RexLambdaRef element = new RexLambdaRef(0, "element", varchar);
    RexNode escapeLambda =
        context.rexBuilder.makeLambdaCall(escapeValue(element, context), List.of(element));
    return context.rexBuilder.makeCall(PPLBuiltinOperators.TRANSFORM, values, escapeLambda);
  }

  private String formatFieldName(String fieldName) {
    if (fieldName.matches("[A-Za-z_@][A-Za-z0-9_@-]*(\\.[A-Za-z_@][A-Za-z0-9_@-]*)*")) {
      return fieldName;
    }
    return "`" + fieldName.replace("`", "``") + "`";
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
