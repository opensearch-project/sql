/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBiVisitorImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeAndFieldIndex;
import org.apache.calcite.rex.RexNormalize;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.Source;

/**
 * This help standardizes the RexNode expression, the process including:
 *
 * <p>1. Replace RexInputRef with RexDynamicParam
 *
 * <p>2. Replace RexLiteral with RexDynamicParam
 *
 * <p>3. Replace RexCall with equivalent functions
 *
 * <p>4. Replace RelDataType with a wider type
 *
 * <p>TODO: 5. Do constant folding
 */
public class RexStandardizer extends RexBiVisitorImpl<RexNode, ScriptParameterHelper> {
  private static final RexStandardizer standardizer = new RexStandardizer(true);

  protected RexStandardizer(boolean deep) {
    super(deep);
  }

  @Override
  public RexNode visitCall(final RexCall call, ScriptParameterHelper helper) {
    // Try to expand `SEARCH` before standardization since we cannot translate literal `Sarg`.
    if (call.op.kind == SqlKind.SEARCH) {
      try {
        return RexUtil.expandSearch(helper.rexBuilder, null, call).accept(this, helper);
      } catch (Exception ignored) {
        // Continue to use the original call if failing to expand.
        // We can downgrade to still use `Sarg` literal instead of replacing it with parameter.
      }
    }
    // Some functions only support limited numeric type. Keep conservative here.
    boolean allowNumericTypeWiden =
        call.op.kind.belongsTo(SqlKind.BINARY_ARITHMETIC)
            || call.op.kind.belongsTo(SqlKind.COMPARISON);
    helper.stack.push(allowNumericTypeWiden);
    try {
      boolean[] update = {false};
      // Do normalization before standardization
      Pair<SqlOperator, List<RexNode>> normalized = RexNormalize.normalize(call.op, call.operands);
      List<RexNode> standardizedOperands = visitList(normalized.right, helper, update);
      return helper.rexBuilder.makeCall(call.getType(), normalized.left, standardizedOperands);
    } finally {
      helper.stack.pop();
    }
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef, ScriptParameterHelper helper) {
    int index = inputRef.getIndex();
    RelDataTypeField field = helper.inputFieldList.get(index);
    ExprType exprType = helper.fieldTypes.get(field.getName());
    String docFieldName =
        exprType == ExprCoreType.STRUCT || exprType == ExprCoreType.ARRAY
            ? null
            : OpenSearchTextType.toKeywordSubField(field.getName(), exprType);
    int newIndex = helper.sources.size();
    if (docFieldName != null) {
      helper.digests.add(docFieldName);
      helper.sources.add(Source.DOC_VALUE.getValue());
    } else {
      helper.digests.add(field.getName());
      helper.sources.add(Source.SOURCE.getValue());
    }
    return new RexDynamicParam(widenType(field.getType(), helper.stack.peek()), newIndex);
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal, ScriptParameterHelper helper) {
    /*
     * 1. Skip replacing SARG/DECIMAL as it is not supported to translate literal;
     * 2. Skip replacing SYMBOL/NULL as it may affect codegen, shouldn't be parameter;
     * 3. Skip INTERVAL_TYPES as it has bug, introduced by calcite-1.41.1, TODO: remove this when fixed;
     */
    if (literal.getTypeName() == SqlTypeName.SARG
        || literal.getTypeName() == SqlTypeName.SYMBOL
        || literal.getTypeName() == SqlTypeName.NULL
        || SqlTypeName.INTERVAL_TYPES.contains(literal.getTypeName())) {
      return literal;
    }

    Object literalValue = translateLiteral(literal);
    if (literalValue == null) return literal;
    int newIndex = helper.sources.size();
    helper.sources.add(Source.LITERAL.getValue());
    helper.digests.add(literalValue);
    return new RexDynamicParam(widenType(literal.getType(), helper.stack.peek()), newIndex);
  }

  /** Override all below method to avoid returning null. */
  @Override
  public RexNode visitLocalRef(RexLocalRef localRef, ScriptParameterHelper arg) {
    return localRef;
  }

  @Override
  public RexNode visitOver(RexOver over, ScriptParameterHelper arg) {
    return over;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable, ScriptParameterHelper arg) {
    return correlVariable;
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam, ScriptParameterHelper arg) {
    return dynamicParam;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef, ScriptParameterHelper arg) {
    return rangeRef;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess, ScriptParameterHelper arg) {
    return fieldAccess;
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery subQuery, ScriptParameterHelper arg) {
    return subQuery;
  }

  @Override
  public RexNode visitTableInputRef(RexTableInputRef ref, ScriptParameterHelper arg) {
    return ref;
  }

  @Override
  public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef, ScriptParameterHelper arg) {
    return fieldRef;
  }

  @Override
  public RexNode visitLambda(RexLambda lambda, ScriptParameterHelper arg) {
    return lambda;
  }

  @Override
  public RexNode visitNodeAndFieldIndex(
      RexNodeAndFieldIndex nodeAndFieldIndex, ScriptParameterHelper arg) {
    return nodeAndFieldIndex;
  }

  protected List<RexNode> visitList(
      List<? extends RexNode> exprs, ScriptParameterHelper helper, boolean[] update) {
    ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
    for (RexNode operand : exprs) {
      RexNode clonedOperand = operand.accept(this, helper);
      if ((clonedOperand != operand) && (update != null)) {
        update[0] = true;
      }
      clonedOperands.add(clonedOperand);
    }
    return clonedOperands.build();
  }

  private static Object translateLiteral(RexLiteral literal) {
    // OpenSearch doesn't accept big decimal value in its script.
    // So try to return its double value directly as we don't really support decimal literal.
    if (SqlTypeUtil.isDecimal(literal.getType()) && literal.getValue() instanceof BigDecimal) {
      // Note: Precision may be lost for values with > 15-17 significant digits
      return ((BigDecimal) literal.getValue()).doubleValue();
    }
    org.apache.calcite.linq4j.tree.Expression expression =
        RexToLixTranslator.translateLiteral(
            literal,
            literal.getType(),
            OpenSearchTypeFactory.TYPE_FACTORY,
            org.apache.calcite.adapter.enumerable.RexImpTable.NullAs.NOT_POSSIBLE);
    if (expression instanceof ConstantExpression) {
      return ((ConstantExpression) expression).value;
    }
    return null;
  }

  /**
   * Widen the input type to a wider and nullable type
   *
   * <p>1. TINYINT, SMALLINT, INTEGER, BIGINT -> BIGINT
   *
   * <p>2. FLOAT, REAL, DOUBLE, DECIMAL -> DOUBLE
   *
   * <p>3. CHAR(*), VARCHAR -> VARCHAR
   *
   * @param type input type
   * @param allowNumericTypeWiden whether allow numeric type widening
   * @return widened type
   */
  private static RelDataType widenType(RelDataType type, Boolean allowNumericTypeWiden) {
    if (type instanceof BasicSqlType) {
      // Downgrade decimal to double since we don't really have that type in core.
      // Using DECIMAL(38, 38) is another choice, but it will be less efficient and isolate the
      // shared cases of types.
      if ((allowNumericTypeWiden == null || allowNumericTypeWiden)
          && (SqlTypeUtil.isDecimal(type) || SqlTypeUtil.isApproximateNumeric(type))) {
        return OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE, true);
      } else if ((allowNumericTypeWiden == null || allowNumericTypeWiden)
          && SqlTypeUtil.isExactNumeric(type)) {
        return OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT, true);
      } else if (SqlTypeUtil.isCharacter(type)) {
        return OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);
      }
    }
    return OpenSearchTypeFactory.TYPE_FACTORY.createTypeWithNullability(type, true);
  }

  public static RexNode standardizeRexNodeExpression(
      RexNode rexNode, ScriptParameterHelper helper) {
    assert helper.stack.isEmpty() : "[BUG]Stack should be empty before standardizing";
    return rexNode.accept(standardizer, helper);
  }
}
