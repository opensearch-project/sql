/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.ConstantExpression;
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
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.Source;

/**
 * This help standardizes the RexNode expression, the process including: 1. Replace RexInputRef with
 * RexDynamicParam 2. Replace RexLiteral with RexDynamicParam TODO: 3. Replace RexCall with
 * equivalent functions 4. Replace RelDataType with a wider type 5. Do constant folding
 */
public class RexStandardizer extends RexBiVisitorImpl<RexNode, ScriptParameterHelper> {
  private static final RexStandardizer standardizer = new RexStandardizer(true);

  protected RexStandardizer(boolean deep) {
    super(deep);
  }

  @Override
  public RexNode visitCall(final RexCall call, ScriptParameterHelper helper) {
    boolean[] update = {false};
    List<RexNode> clonedOperands = visitList(call.operands, helper, update);
    if (update[0]) {
      // REVIEW jvs 8-Mar-2005:  This doesn't take into account
      // the fact that a rewrite may have changed the result type.
      // To do that, we would need to take a RexBuilder and
      // watch out for special operators like CAST and NEW where
      // the type is embedded in the original call.
      return call.clone(call.getType(), clonedOperands);
    } else {
      return call;
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
    int newIndex = helper.currentIndex[0]++;
    if (docFieldName != null) {
      helper.digests.add(docFieldName);
      helper.sources.add(Source.DOC_VALUE.getValue());
    } else {
      helper.digests.add(field.getName());
      helper.sources.add(Source.SOURCE.getValue());
    }
    return new RexDynamicParam(field.getType(), newIndex);
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal, ScriptParameterHelper helper) {
    /*
     * 1. Skip replacing SARG/DECIMAL as it is not supported to translate literal;
     * 2. Skip replacing SYMBOL/NULL as it may affect codegen, shouldn't be parameter;
     * 3. Skip INTERVAL_TYPES as it has bug, introduced by calcite-1.41.1, TODO: remove this when fixed;
     */
    if (literal.getTypeName() == SqlTypeName.SARG
        || literal.getType().getSqlTypeName() == SqlTypeName.DECIMAL
        || literal.getTypeName() == SqlTypeName.SYMBOL
        || literal.getTypeName() == SqlTypeName.NULL
        || SqlTypeName.INTERVAL_TYPES.contains(literal.getTypeName())) {
      return literal;
    }

    Object literalValue = translateLiteral(literal);
    if (literalValue == null) return literal;
    int newIndex = helper.currentIndex[0]++;
    helper.sources.add(Source.LITERAL.getValue());
    if (helper.literals.contains(literalValue)) {
      helper.digests.add(helper.literals.indexOf(literalValue));
    } else {
      helper.digests.add(helper.literals.size());
      helper.literals.add(literalValue);
    }
    return new RexDynamicParam(literal.getType(), newIndex);
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
    org.apache.calcite.linq4j.tree.Expression expression =
        RexToLixTranslator.translateLiteral(
            literal,
            literal.getType(),
            OpenSearchTypeFactory.TYPE_FACTORY,
            org.apache.calcite.adapter.enumerable.RexImpTable.NullAs.NOT_POSSIBLE);
    if (expression instanceof ConstantExpression constantExpression) {
      return constantExpression.value;
    }
    return null;
  }

  public static RexNode standardizeRexNodeExpression(
      RexNode rexNode, ScriptParameterHelper helper) {
    return rexNode.accept(standardizer, helper);
  }
}
