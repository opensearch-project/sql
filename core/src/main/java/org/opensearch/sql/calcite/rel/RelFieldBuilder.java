/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rel;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.ExtendedRexBuilder;
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
  private final ExtendedRexBuilder rexBuilder;

  public List<String> getStaticFieldNames() {
    return getStaticFieldNames(0);
  }

  public List<String> getStaticFieldNames(int n) {
    return getAllFieldNames(n).stream()
        .filter(RelFieldBuilder::isStaticField)
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

  public List<RexNode> allFields() {
    return relBuilder.fields();
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
    List<String> originalFields = relBuilder.peek().getRowType().getFieldNames();
    return IntStream.range(0, originalFields.size())
        .filter(i -> isStaticField(originalFields.get(i)))
        .mapToObj(i -> relBuilder.field(i))
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

  private static boolean isStaticField(String fieldName) {
    // use startsWith as `_MAP` can be renamed to `_MAP0`, etc. by RelBuilder
    return !fieldName.startsWith(DYNAMIC_FIELDS_MAP);
  }

  @Value
  @AllArgsConstructor
  private static class JoinedField {
    boolean isInLeft;
    boolean isInRight;
    boolean isDynamicFieldMap;
    String fieldName;
    RexInputRef fieldNode;
  }

  /**
   * Reorganize dynamic fields map (_MAP) after join. It will concat dynamic fields map from both
   * side, and also override static field by the value from dynamic fields as needed. This should be
   * called after join, but with fields information before join as parameters
   */
  public void reorganizeDynamicFields(List<String> leftAllFields, List<String> rightAllFields) {
    boolean leftHasDynamicFields = leftAllFields.contains(DYNAMIC_FIELDS_MAP);
    boolean rightHasDynamicFields = rightAllFields.contains(DYNAMIC_FIELDS_MAP);

    if (!leftHasDynamicFields && !rightHasDynamicFields) {
      return;
    }

    List<JoinedField> fields = collectStaticFieldsInfo(leftAllFields, rightAllFields);
    Optional<JoinedField> leftMap =
        leftHasDynamicFields ? Optional.of(getLeftDynamicFieldMapInfo()) : Optional.empty();
    Optional<JoinedField> rightMap =
        rightHasDynamicFields ? Optional.of(getRightDynamicFieldMapInfo()) : Optional.empty();

    List<RexNode> projected = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (JoinedField field : fields) {
      if (field.isInLeft && !field.isInRight && rightHasDynamicFields) {
        // need to prioritize value from right dynamic map if only left side has static field with
        // the name
        RexNode valueFromRightMap = createItemAccess(rightMap.get().fieldNode, field.fieldName);
        RexNode merged = rexBuilder.coalesce(valueFromRightMap, field.fieldNode);
        projected.add(merged);
        names.add(field.fieldName);
      } else {
        projected.add(field.fieldNode);
        names.add(field.fieldName);
      }
    }
    if (leftHasDynamicFields && rightHasDynamicFields) {
      // need to concat map from both side
      projected.add(mapConcatCall(leftMap.get().fieldNode, rightMap.get().fieldNode));
    } else if (leftHasDynamicFields) {
      projected.add(leftMap.get().fieldNode);
    } else if (rightHasDynamicFields) {
      projected.add(rightMap.get().fieldNode);
    }
    names.add(DYNAMIC_FIELDS_MAP);

    relBuilder.projectNamed(projected, names, true);
  }

  private JoinedField getLeftDynamicFieldMapInfo() {
    List<String> allFields = getAllFieldNames();
    for (int i = 0; i < allFields.size(); i++) {
      String fieldName = allFields.get(i);
      if (fieldName.startsWith(DYNAMIC_FIELDS_MAP)) {
        return new JoinedField(true, false, true, fieldName, relBuilder.field(i));
      }
    }
    throw new IllegalStateException("Dynamic field not found for right input.");
  }

  private JoinedField getRightDynamicFieldMapInfo() {
    List<String> allFields = getAllFieldNames();
    for (int i = allFields.size() - 1; 0 <= i; i++) {
      String fieldName = allFields.get(i);
      if (fieldName.startsWith(DYNAMIC_FIELDS_MAP)) {
        return new JoinedField(false, true, true, fieldName, relBuilder.field(i));
      }
    }
    throw new IllegalStateException("Dynamic field not found for right input.");
  }

  private List<JoinedField> collectStaticFieldsInfo(
      List<String> leftAllFields, List<String> rightAllFields) {
    List<JoinedField> fields = new ArrayList<>();
    List<String> allFields = getAllFieldNames();
    // iterate by index to avoid referring field by name (it causes issue when field name is
    // overlapping)
    for (int i = 0; i < allFields.size(); i++) {
      String fieldName = allFields.get(i);
      if (!fieldName.startsWith(DYNAMIC_FIELDS_MAP)) {
        boolean isInLeft = leftAllFields.contains(fieldName);
        boolean isInRight =
            rightAllFields.contains(fieldName)
                || (isInLeft && rightAllFields.contains(excludeLastZero(fieldName)));
        fields.add(new JoinedField(isInLeft, isInRight, false, fieldName, relBuilder.field(i)));
      }
    }
    return fields;
  }

  /** exclude zero to consider the rename (adding `0` at the end) due to duplicate name */
  private String excludeLastZero(String fieldName) {
    return fieldName.endsWith("0") ? fieldName.substring(0, fieldName.length() - 1) : fieldName;
  }

  private RexNode mapConcatCall(RexInputRef left, RexInputRef right) {
    return PPLFuncImpTable.INSTANCE.resolve(
        rexBuilder, BuiltinFunctionName.MAP_CONCAT, left, right);
  }
}
