/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.TableFunctionCallImplementor;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.patterns.PatternUtils;

public class SqlUncollectPatternsTableFunction extends SqlWindowTableFunction {

  public SqlUncollectPatternsTableFunction() {
    super("UNCOLLECT_PATTERNS", new OperandMetadataImpl());
  }

  public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_UNCOLLECT =
      SqlUncollectPatternsTableFunction::inferRowType;

  private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
    final RelDataType inputRowType = opBinding.getOperandType(0);
    return opBinding
        .getTypeFactory()
        .builder()
        .kind(inputRowType.getStructKind())
        .addAll(inputRowType.getFieldList())
        .add(PatternUtils.PATTERN, SqlTypeName.VARCHAR)
        .add(PatternUtils.PATTERN_COUNT, SqlTypeName.BIGINT)
        .add(PatternUtils.TOKENS, UserDefinedFunctionUtils.tokensMap)
        .build();
  }

  @Override
  public SqlReturnTypeInference getRowTypeInference() {
    return ARG0_TABLE_FUNCTION_UNCOLLECT;
  }

  public static class UncollectPatternsImplementor implements TableFunctionCallImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator,
        Expression inputEnumerable,
        RexCall rexCall,
        PhysType inputPhysType,
        PhysType outputPhysType) {

      final RexLiteral ordinalLiteral = (RexLiteral) rexCall.getOperands().get(1);
      final int ordinal = ((BigDecimal) ordinalLiteral.getValue()).intValue();
      final Expression ordinalExpr = Expressions.constant(ordinal, int.class);

      return Expressions.call(
          Types.lookupMethod(
              UncollectPatternsImplementor.class, "uncollect", Enumerator.class, int.class),
          Expressions.list(
              Expressions.call(inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method),
              ordinalExpr));
    }

    public static Enumerable<@Nullable Object[]> uncollect(
        Enumerator<@Nullable Object[]> inputEnumerator, int ordinal) {
      return new AbstractEnumerable<@Nullable Object[]>() {
        @Override
        public Enumerator<@Nullable Object[]> enumerator() {
          return new UncollectEnumerator(inputEnumerator, ordinal);
        }
      };
    }
  }

  private static class UncollectEnumerator implements Enumerator<@Nullable Object[]> {
    private final Enumerator<@Nullable Object[]> inputEnumerator;
    private final int ordinal;
    private final Deque<@Nullable Object[]> list;

    UncollectEnumerator(Enumerator<@Nullable Object[]> inputEnumerator, int ordinal) {
      this.inputEnumerator = inputEnumerator;
      this.ordinal = ordinal;
      this.list = new ArrayDeque<>();
    }

    @Override
    public @Nullable Object[] current() {
      if (!list.isEmpty()) {
        return this.takeOne();
      } else {
        @Nullable Object currentObject = inputEnumerator.current();
        Object[] current =
            currentObject instanceof List<?>
                ? new Object[] {currentObject}
                : (Object[]) currentObject;
        Object aggResult = current[ordinal];
        if (aggResult == null) {
          @Nullable Object[] currentWithPatterns = new Object[current.length + 3];
          System.arraycopy(current, 0, currentWithPatterns, 0, current.length);
          currentWithPatterns[current.length] = null;
          currentWithPatterns[current.length + 1] = null;
          currentWithPatterns[current.length + 2] = null;
          list.offer(currentWithPatterns);
        } else {
          List<Map<String, Object>> patternMaps = (List<Map<String, Object>>) aggResult;
          for (Map<String, Object> patternMap : patternMaps) {
            @Nullable Object[] currentWithPatterns = new Object[current.length + 3];
            System.arraycopy(current, 0, currentWithPatterns, 0, current.length);
            currentWithPatterns[current.length] = patternMap.get(PatternUtils.PATTERN);
            currentWithPatterns[current.length + 1] = patternMap.get(PatternUtils.PATTERN_COUNT);
            currentWithPatterns[current.length + 2] = patternMap.get(PatternUtils.TOKENS);
            list.offer(currentWithPatterns);
          }
        }
      }
      return takeOne();
    }

    @Override
    public boolean moveNext() {
      return !list.isEmpty() || inputEnumerator.moveNext();
    }

    @Override
    public void reset() {
      inputEnumerator.reset();
      list.clear();
    }

    @Override
    public void close() {}

    private @Nullable Object[] takeOne() {
      return Objects.requireNonNull(list.pollFirst(), "list.pollFirst()");
    }
  }

  private static class OperandMetadataImpl implements SqlOperandMetadata {

    OperandMetadataImpl() {}

    @Override
    public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean b) {
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.from(1);
    }

    @Override
    public String getAllowedSignatures(SqlOperator sqlOperator, String opName) {
      return opName + "(TABLE data)";
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory relDataTypeFactory) {
      return List.of();
    }

    @Override
    public List<String> paramNames() {
      return List.of();
    }
  }
}
