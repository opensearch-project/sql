/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rel;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Wrapper for RelBuilder to handle field operations considering dynamic fields. It provides
 * explicit methods to access static fields or dynamic fields, and also helper methods to handle
 * dynamic fields operations.
 */
@RequiredArgsConstructor
public class RelFieldBuilder {
  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;

  public List<String> getStaticFieldNames() {
    return getStaticFieldNames(0);
  }

  public List<String> getStaticFieldNames(int n) {
    return getAllFieldNames(n).stream()
        .filter(name -> !DYNAMIC_FIELDS_MAP.equals(name))
        .collect(Collectors.toList());
  }

  public List<String> getAllFieldNames() {
    return getAllFieldNames(0);
  }

  public List<String> getAllFieldNames(int inputCount, int inputOrdinal) {
    return getAllFieldNames(getStackPosition(inputCount, inputOrdinal));
  }

  public int getStackPosition(int inputCount, int inputOrdinal) {
    return inputCount - 1 - inputOrdinal;
  }

  public List<String> getAllFieldNames(int n) {
    return relBuilder.peek(n).getRowType().getFieldNames();
  }

  public RexInputRef staticField(String fieldName) {
    return relBuilder.field(fieldName);
  }

  public RexInputRef staticField(int inputCount, int inputOrdinal, String fieldName) {
    return relBuilder.field(inputCount, inputOrdinal, fieldName);
  }

  public RexInputRef staticField(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return relBuilder.field(inputCount, inputOrdinal, fieldOrdinal);
  }

  public RexInputRef staticField(int fieldIndex) {
    return relBuilder.field(fieldIndex);
  }

  public List<RexNode> staticFields() {
    return relBuilder.fields().stream()
        .filter(node -> !DYNAMIC_FIELDS_MAP.equals(node))
        .collect(Collectors.toList());
  }

  public List<RexNode> staticFields(Iterable<String> fieldNames) {
    return relBuilder.fields(fieldNames);
  }

  public List<RelDataTypeField> staticFieldList() {
    return relBuilder.peek().getRowType().getFieldList().stream()
        .filter(field -> !DYNAMIC_FIELDS_MAP.equals(field.getName()))
        .toList();
  }

  public ImmutableList<RexNode> staticFields(List<? extends Number> ordinals) {
    return relBuilder.fields(ordinals);
  }

  public boolean isDynamicFieldsExist() {
    return isDynamicFieldsExist(1, 0);
  }

  public boolean isDynamicFieldsExist(int inputCount, int inputOrdinal) {
    return getAllFieldNames(inputCount, inputOrdinal).contains(DYNAMIC_FIELDS_MAP);
  }

  public List<RexNode> metaFieldsRef() {
    List<String> originalFields = relBuilder.peek().getRowType().getFieldNames();
    return originalFields.stream()
        .filter(OpenSearchConstants.METADATAFIELD_TYPE_MAP::containsKey)
        .map(metaField -> (RexNode) relBuilder.field(metaField))
        .toList();
  }

  public RexNode correlField(RexNode correlNode, int index) {
    return relBuilder.field(correlNode, index);
  }

  public RexNode dynamicField(String fieldName) {
    return dynamicField(1, 0, fieldName);
  }

  public RexNode getDynamicFieldsMap() {
    return relBuilder.field(DYNAMIC_FIELDS_MAP);
  }

  public RexNode dynamicField(int inputCount, int inputOrdinal, String fieldName) {
    return createItemAccess(
        relBuilder.field(inputCount, inputOrdinal, DYNAMIC_FIELDS_MAP), fieldName);
  }

  private RexNode createItemAccess(RexNode field, String itemName) {
    return PPLFuncImpTable.INSTANCE.resolve(
        rexBuilder, BuiltinFunctionName.INTERNAL_ITEM, field, rexBuilder.makeLiteral(itemName));
  }
}
