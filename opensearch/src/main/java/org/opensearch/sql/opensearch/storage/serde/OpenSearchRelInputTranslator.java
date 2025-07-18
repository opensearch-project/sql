/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * InputTranslator translates RelInput to specific RexInputRef given slot index. Assumes the
 * expression directly reads the scanned output when RexNode is pushed down into Scan, only the row
 * type {@link RelDataType} of input is required to locate the input reference.
 */
public class OpenSearchRelInputTranslator implements RelJson.InputTranslator {

  private final RelDataType rowType;

  public OpenSearchRelInputTranslator(RelDataType rowType) {
    this.rowType = rowType;
  }

  @Override
  public RexNode translateInput(
      RelJson relJson, int input, Map<String, @Nullable Object> map, RelInput relInput) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    if (input < rowType.getFieldCount()) {
      final RelDataTypeField field = rowType.getFieldList().get(input);
      return rexBuilder.makeInputRef(field.getType(), input);
    }
    throw new RuntimeException("input field " + input + " is out of range");
  }
}
