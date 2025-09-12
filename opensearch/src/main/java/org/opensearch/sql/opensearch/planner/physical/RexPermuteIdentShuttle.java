/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import static java.util.Objects.hash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.AbstractTargetMapping;

public class RexPermuteIdentShuttle extends RexPermuteInputsShuttle {
  private final IdentTargetMapping mapping;
  private final RexVisitorImpl<Ident> visitor;

  public RexPermuteIdentShuttle(IdentTargetMapping mapping, RexVisitorImpl<Ident> visitor) {
    super(mapping);
    this.mapping = mapping;
    this.visitor = visitor;
  }

  public static RexPermuteIdentShuttle of(
      IdentTargetMapping mapping, RexVisitorImpl<Ident> visitor) {
    return new RexPermuteIdentShuttle(mapping, visitor);
  }

  @Override
  public RexNode visitInputRef(RexInputRef local) {
    return visitToTransform(local);
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return visitToTransform(fieldAccess);
  }

  private RexNode visitToTransform(RexNode node) {
    Ident ident = node.accept(visitor);
    int target = mapping.getTargetByIdent(ident);
    return new RexInputRef(target, node.getType());
  }

  /**
   * @param prefix The prefix of the Ident, null if the Ident is generated from RexInputRef
   * @param index The index of the Ident in the schema
   * @param typeField For the convenience of retrieving RelDataTypeField of RexFieldAccess; Should
   *     be NULL for RexInputRef since the field name has been set to index, which is useless. No
   *     usage in identification of Ident, so no need to be used in equals or hashCode.
   */
  public record Ident(
      RexPermuteIdentShuttle.Ident prefix, Integer index, RelDataTypeField typeField) {

    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Ident otherIdent)) {
        return false;
      }
      return Objects.equals(prefix, otherIdent.prefix) && Objects.equals(index, otherIdent.index);
    }

    public int hashCode() {
      return hash(prefix, index);
    }

    public RelDataTypeField constructFieldDataType(List<RelDataTypeField> fieldList) {
      // For RexInputRef, the typeField is null, so we use the retrieved field in the row type.
      return prefix == null
          ? fieldList.get(index)
          : new RelDataTypeFieldImpl(constructFieldName(fieldList), index, typeField.getType());
    }

    private String constructFieldName(List<RelDataTypeField> fieldList) {
      return prefix == null
          ? fieldList.get(index).getName()
          : prefix.constructFieldName(fieldList) + "." + typeField.getName();
    }
  }

  public static class IdentTargetMapping extends AbstractTargetMapping {
    private final Map<Ident, Integer> identToTarget;

    private IdentTargetMapping(Map<Ident, Integer> identToTarget, int sourceCount) {
      super(sourceCount, identToTarget.size());
      this.identToTarget = identToTarget;
    }

    public static IdentTargetMapping create(List<Ident> selectedColumns, int sourceCount) {
      final Map<Ident, Integer> identToTarget = new HashMap<>();
      for (int i = 0; i < selectedColumns.size(); i++) {
        identToTarget.put(selectedColumns.get(i), i);
      }
      return new IdentTargetMapping(identToTarget, sourceCount);
    }

    @Override
    public int getTargetOpt(int source) {
      throw new UnsupportedOperationException();
    }

    public int getTargetByIdent(Ident ident) {
      return identToTarget.getOrDefault(ident, -1);
    }
  }

  public static class IdentRexVisitorImpl extends RexVisitorImpl<Ident> {
    protected IdentRexVisitorImpl() {
      super(true);
    }

    @Override
    public Ident visitInputRef(RexInputRef inputRef) {
      return new Ident(null, inputRef.getIndex(), null);
    }

    @Override
    public Ident visitFieldAccess(RexFieldAccess fieldAccess) {
      Ident prefix = fieldAccess.getReferenceExpr().accept(this);
      return new Ident(prefix, fieldAccess.getField().getIndex(), fieldAccess.getField());
    }
  }
}
