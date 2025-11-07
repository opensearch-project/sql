/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBiVisitorImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

@UtilityClass
public class OpenSearchRelOptUtil {
  private static final RemapIndexBiVisitor remapIndexBiVisitor = new RemapIndexBiVisitor(true);

  /**
   * For pushed down RexNode, the input schema doesn't need to be the same with scan output schema
   * because the input values are read from ScriptDocValues or source by field name. It doesn't
   * matter what the actual index is. Current serialization will serialize map of field name and
   * field ExprType, which could be a long serialized string. Use this method to narrow down input
   * rowType and rewrite RexNode's input references. After that, we can leverage the fewer columns
   * in the rowType to serialize least required field types.
   *
   * @param rexNode original RexNode to be pushed down
   * @param inputRowType original input rowType of RexNode
   * @return rewritten pair of RexNode and RelDataType
   */
  public static Pair<RexNode, RelDataType> getRemappedRexAndType(
      final RexNode rexNode, final RelDataType inputRowType) {
    final BitSet seenOldIndex = new BitSet();
    final List<Integer> newMappings = new ArrayList<>();
    rexNode.accept(remapIndexBiVisitor, Pair.of(seenOldIndex, newMappings));
    final List<RelDataTypeField> inputFieldList = inputRowType.getFieldList();
    final RelDataTypeFactory.Builder builder = OpenSearchTypeFactory.TYPE_FACTORY.builder();
    for (Integer oldIdx : newMappings) {
      builder.add(inputFieldList.get(oldIdx));
    }
    final Mapping mapping = Mappings.target(newMappings, inputRowType.getFieldCount());
    final RexNode newMappedRex = RexUtil.apply(mapping, rexNode);
    return Pair.of(newMappedRex, builder.build());
  }

  public static Pair<RexNode, RelDataType> getRemappedRexAndType2(
      final RexNode rexNode, final RelDataType inputRowType, final Map<String, ExprType> fieldTypes,
      List<String> sourceOnlyFields, List<RexLiteral> literals) {
    final List<RelDataTypeField> inputFieldList = inputRowType.getFieldList();
    final RelDataTypeFactory.Builder builder = OpenSearchTypeFactory.TYPE_FACTORY.builder();

    final Set<Integer> visitedFields = new HashSet<>();
    final Set<RexLiteral> visitedLiterals = new HashSet<>();
    final RexVisitorImpl<Void> visitor =
        new RexVisitorImpl<>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            visitedFields.add(index);
            RelDataTypeField field = inputFieldList.get(index);
            ExprType exprType = fieldTypes.get(field.getName());
            String docFieldName =
                exprType == ExprCoreType.STRUCT || exprType == ExprCoreType.ARRAY ?
                null : OpenSearchTextType.toKeywordSubField(field.getName(), exprType);
            if (docFieldName != null) {
              builder.add(docFieldName, field.getType());
            } else {
              sourceOnlyFields.add(field.getName());
              builder.add(field.getName(), field.getType());
            }
            return null;
          }

          @Override
          public Void visitLiteral(RexLiteral literal) {
            visitedLiterals.add(literal);
            return null;
          }
        };
    rexNode.accept(visitor);

    final Mapping mapping = Mappings.target(visitedFields.stream().toList(), inputRowType.getFieldCount());
    literals.addAll(visitedLiterals);
    final Map<RexLiteral, Integer> literalToIndex = IntStream.range(0, literals.size()).boxed()
        .collect(Collectors.toMap(
            literals::get,
            i -> i,
            (existingValue, newValue) -> existingValue
        ));
    return Pair.of(rexNode.accept(MyRexPermuteInputsShuttle.of(mapping, literalToIndex)), builder.build());
  }

  static class MyRexPermuteInputsShuttle extends RexPermuteInputsShuttle {
    final int fieldCount;
    final Map<RexLiteral, Integer> literals;

    public static MyRexPermuteInputsShuttle of(TargetMapping mapping,
        Map<RexLiteral, Integer> literals) {
      return new MyRexPermuteInputsShuttle(mapping, literals);
    }

    public MyRexPermuteInputsShuttle(TargetMapping mapping, Map<RexLiteral, Integer> literals) {
      super(mapping);
      this.fieldCount = mapping.getTargetCount();
      this.literals = literals;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      return new RexInputRef(literals.get(literal) + fieldCount, literal.getType());
    }
  }

  /**
   * Given an input Calcite RexNode, find the single input field with equivalent collation
   * information. The function returns the pair of input field index and a flag to indicate whether
   * the ordering is flipped.
   *
   * @param expr Calcite expression node
   * @return Optional pair of input field index and flipped flag
   */
  public static Optional<Pair<Integer, Boolean>> getOrderEquivalentInputInfo(RexNode expr) {
    switch (expr.getKind()) {
      case INPUT_REF:
        RexInputRef inputRef = (RexInputRef) expr;
        return Optional.of(Pair.of(inputRef.getIndex(), false));
      case PLUS_PREFIX:
        return getOrderEquivalentInputInfo(((RexCall) expr).getOperands().get(0));
      case MINUS_PREFIX:
        return getOrderEquivalentInputInfo(((RexCall) expr).getOperands().get(0))
            .map(inputInfo -> Pair.of(inputInfo.getLeft(), !inputInfo.getRight()));
      case PLUS, MINUS:
      {
        RexNode operand0 = ((RexCall) expr).getOperands().get(0);
        RexNode operand1 = ((RexCall) expr).getOperands().get(1);

        boolean operand0Lit = operand0.isA(SqlKind.LITERAL);
        boolean operand1Lit = operand1.isA(SqlKind.LITERAL);

        if (operand0Lit == operand1Lit) {
          return Optional.empty();
        }

        RexNode variable = operand0Lit ? operand1 : operand0;
        boolean flipped = (expr.getKind() == SqlKind.MINUS) && operand0Lit;

        return getOrderEquivalentInputInfo(variable)
            .map(inputInfo -> Pair.of(inputInfo.getLeft(), flipped != inputInfo.getRight()));
      }
      case TIMES:
      {
        RexNode operand0 = ((RexCall) expr).getOperands().get(0);
        RexNode operand1 = ((RexCall) expr).getOperands().get(1);

        RexNode lit =
            operand0.isA(SqlKind.LITERAL)
                ? operand0
                : (operand1.isA(SqlKind.LITERAL) ? operand1 : null);
        RexNode variable = (lit == operand0) ? operand1 : operand0;

        if (lit == null) {
          return Optional.empty();
        }

        BigDecimal k = ((RexLiteral) lit).getValueAs(BigDecimal.class);
        if (k == null || k.signum() == 0) {
          return Optional.empty();
        }
        boolean flipped = k.signum() < 0;

        return getOrderEquivalentInputInfo(variable)
            .map(inputInfo -> Pair.of(inputInfo.getLeft(), flipped != inputInfo.getRight()));
      }
      // Ignore DIVIDE operator for now because it has too many precision issues
      case CAST, SAFE_CAST:
      {
        RexNode child = ((RexCall) expr).getOperands().get(0);
        if (!isOrderPreservingCast(child.getType(), expr.getType())) {
          return Optional.empty();
        }
        return getOrderEquivalentInputInfo(child);
      }
      default:
        return Optional.empty();
    }
  }

  private static boolean isOrderPreservingCast(RelDataType src, RelDataType dst) {
    final SqlTypeName srcType = src.getSqlTypeName();
    final SqlTypeName dstType = dst.getSqlTypeName();

    if (SqlTypeUtil.isIntType(src) && SqlTypeUtil.isApproximateNumeric(dst)) {
      int intBits =
          switch (srcType) {
            case TINYINT -> 8;
            case SMALLINT -> 16;
            case INTEGER -> 32;
            case BIGINT -> 64;
            default -> 0;
          };
      // Float and double can only handle exact number based on its significand precision
      int floatBits =
          switch (dstType) {
            case FLOAT -> 24;
            case DOUBLE -> 53;
            default -> 0;
          };
      return intBits > 0 && floatBits > 0 && intBits <= floatBits;
    }

    if (SqlTypeUtil.isExactNumeric(src) && SqlTypeUtil.isExactNumeric(dst)) {
      int srcPrec = src.getPrecision();
      int dstPrec = dst.getPrecision();
      return dstPrec >= srcPrec;
    }

    if (SqlTypeUtil.isCharacter(src) && SqlTypeUtil.isCharacter(dst)) {
      int srcLength = src.getPrecision();
      int dstLength = dst.getPrecision();
      return dstLength >= srcLength || dstLength == RelDataType.PRECISION_NOT_SPECIFIED;
    }

    if (srcType == SqlTypeName.DATE
        && (dstType == SqlTypeName.TIMESTAMP
        || dstType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
      return true;
    }

    if (srcType == SqlTypeName.TIME
        && (dstType == SqlTypeName.TIMESTAMP
        || dstType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
      return true;
    }

    if (srcType == dstType) {
      return dst.getPrecision() >= src.getPrecision() && dst.getScale() >= src.getScale();
    }

    return false;
  }

  private static class RemapIndexBiVisitor
      extends RexBiVisitorImpl<Void, Pair<BitSet, List<Integer>>> {
    protected RemapIndexBiVisitor(boolean deep) {
      super(deep);
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef, Pair<BitSet, List<Integer>> args) {
      final BitSet seenOldIndex = args.getLeft();
      final List<Integer> newMappings = args.getRight();
      final int oldIdx = inputRef.getIndex();
      if (!seenOldIndex.get(oldIdx)) {
        seenOldIndex.set(oldIdx);
        newMappings.add(oldIdx);
      }
      return null;
    }
  }

  /**
   * Replace dot in field name with underscore, since Calcite has bug in codegen if a field name
   * contains dot.
   *
   * <p>Fields replacement examples:
   *
   * <p>a_b, a.b -> a_b, a_b0
   *
   * <p>a_b, a_b0, a.b -> a_b, a_b0, a_b1
   *
   * <p>a_b, a_b1, a.b -> a_b, a_b1, a_b0
   *
   * <p>a_b0, a.b0, a.b1 -> a_b0, a_b00, a_b1
   *
   * @param rowType RowType
   * @return RowType with field name replaced
   */
  public RelDataType replaceDot(RelDataTypeFactory typeFactory, RelDataType rowType) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final List<RelDataTypeField> fieldList = rowType.getFieldList();
    List<String> originalNames = new ArrayList<>();
    for (RelDataTypeField field : fieldList) {
      originalNames.add(field.getName());
    }
    List<String> resolvedNames = OpenSearchRelOptUtil.resolveColumnNameConflicts(originalNames);
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      builder.add(
          new RelDataTypeFieldImpl(resolvedNames.get(i), field.getIndex(), field.getType()));
    }
    return builder.build();
  }

  public static List<String> resolveColumnNameConflicts(List<String> originalNames) {
    List<String> result = new ArrayList<>(originalNames);
    Set<String> usedNames = new HashSet<>(originalNames);
    for (int i = 0; i < originalNames.size(); i++) {
      String originalName = originalNames.get(i);
      if (originalName.contains(".")) {
        String baseName = originalName.replace('.', '_');
        String newName = generateUniqueName(baseName, usedNames);
        result.set(i, newName);
        usedNames.add(newName);
      }
    }
    return result;
  }

  private static String generateUniqueName(String baseName, Set<String> usedNames) {
    if (!usedNames.contains(baseName)) {
      return baseName;
    }
    String candidate = baseName + "0";
    if (!usedNames.contains(candidate)) {
      return candidate;
    }
    int suffix = 1;
    while (true) {
      candidate = baseName + suffix;
      if (!usedNames.contains(candidate)) {
        return candidate;
      }
      suffix++;
    }
  }
}
