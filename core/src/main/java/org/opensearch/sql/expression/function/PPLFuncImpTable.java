/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static java.lang.Math.E;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ABS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ACOS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.AND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ASCII;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ASIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ATAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ATAN2;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CBRT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CEILING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONCAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONCAT_WS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DEGREES;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EXP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FLOOR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GREATER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_REGEXP_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_3;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LEFT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LESS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOG;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOG10;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOG2;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOWER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LTRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOTEQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.OR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.PATTERN_PARSER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.PI;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POSITION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POW;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POWER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RADIANS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RAND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REGEXP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REVERSE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RIGHT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ROUND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RTRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SIGN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SPAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STRCMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBSTR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBSTRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TYPEOF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UNCOLLECT_PATTERNS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UPPER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.XOR;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.TableFunctionCallImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.SqlUncollectPatternsTableFunction.UncollectPatternsImplementor;
import org.opensearch.sql.utils.ReflectionUtils;

public class PPLFuncImpTable {

  public interface FunctionImp {
    RelDataType ANY_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);

    RexNode resolve(RexBuilder builder, RexNode... args);

    /**
     * @return the list of parameters. Default return null implies unknown parameters {@link
     *     CalciteFuncSignature} won't check parameters if it's null
     */
    default List<RelDataType> getParams() {
      return null;
    }
  }

  public interface FunctionImp1 extends FunctionImp {
    List<RelDataType> ANY_TYPE_1 = List.of(ANY_TYPE);

    RexNode resolve(RexBuilder builder, RexNode arg1);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("This function requires exactly 1 arguments");
      }
      return resolve(builder, args[0]);
    }

    @Override
    default List<RelDataType> getParams() {
      return ANY_TYPE_1;
    }
  }

  public interface FunctionImp2 extends FunctionImp {
    List<RelDataType> ANY_TYPE_2 = List.of(ANY_TYPE, ANY_TYPE);

    RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 2) {
        throw new IllegalArgumentException("This function requires exactly 2 arguments");
      }
      return resolve(builder, args[0], args[1]);
    }

    @Override
    default List<RelDataType> getParams() {
      return ANY_TYPE_2;
    }
  }

  /** The singleton instance. */
  public static final PPLFuncImpTable INSTANCE;

  static {
    final Builder builder = new Builder();
    builder.populate();
    INSTANCE = new PPLFuncImpTable(builder);
  }

  private final ImmutableMap<BuiltinFunctionName, PairList<CalciteFuncSignature, FunctionImp>> map;

  private PPLFuncImpTable(Builder builder) {
    final ImmutableMap.Builder<BuiltinFunctionName, PairList<CalciteFuncSignature, FunctionImp>>
        mapBuilder = ImmutableMap.builder();
    builder.map.forEach((k, v) -> mapBuilder.put(k, v.immutable()));
    this.map = ImmutableMap.copyOf(mapBuilder.build());
  }

  public @Nullable RexNode resolveSafe(
      final RexBuilder builder, final String functionName, RexNode... args) {
    try {
      return resolve(builder, functionName, args);
    } catch (Exception e) {
      return null;
    }
  }

  public RexNode resolve(final RexBuilder builder, final String functionName, RexNode... args) {
    Optional<BuiltinFunctionName> funcNameOpt = BuiltinFunctionName.of(functionName);
    if (funcNameOpt.isEmpty()) {
      throw new IllegalArgumentException(String.format("Unsupported function: %s", functionName));
    }
    return resolve(builder, funcNameOpt.get(), args);
  }

  public RexNode resolve(
      final RexBuilder builder, final BuiltinFunctionName functionName, RexNode... args) {
    final PairList<CalciteFuncSignature, FunctionImp> implementList = map.get(functionName);
    if (implementList == null || implementList.isEmpty()) {
      throw new IllegalStateException(String.format("Cannot resolve function: %s", functionName));
    }
    List<RelDataType> argTypes = Arrays.stream(args).map(RexNode::getType).toList();
    try {
      for (Map.Entry<CalciteFuncSignature, FunctionImp> implement : implementList) {
        if (implement.getKey().match(functionName.getName(), argTypes)) {
          return implement.getValue().resolve(builder, args);
        }
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot resolve function: %s, arguments: %s, caused by: %s",
              functionName, argTypes, e.getMessage()),
          e);
    }
    throw new IllegalArgumentException(
        String.format("Cannot resolve function: %s, arguments: %s", functionName, argTypes));
  }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private abstract static class AbstractBuilder {

    /** Maps an operator to an implementation. */
    abstract void register(BuiltinFunctionName functionName, FunctionImp functionImp);

    void registerOperator(BuiltinFunctionName functionName, SqlOperator operator) {
      register(
          functionName, (RexBuilder builder, RexNode... node) -> builder.makeCall(operator, node));
    }

    /*
     * Caveat!: Hacky way to modify Calcite RexImpTable tvfMap via reflection.
     * We do this because Calcite exposed UserDefinedTableFunction's implementor only accepts RexCall
     * without inputEnumerable. It's more useful to defined UDTF with predefined values instead of child
     * plan's input. So we extend SqlWindowTableFunction to implement our own table function.
     */
    void registerTvfIntoRexImpTable() {
      RexImpTable rexImpTable = RexImpTable.INSTANCE;

      Field tvfMapField = ReflectionUtils.getDeclaredField(rexImpTable, "tvfImplementorMap");
      assert tvfMapField != null
          : String.format(
              Locale.ROOT, "Could not get field: tvfImplementorMap on object: %s", rexImpTable);
      ReflectionUtils.makeAccessible(tvfMapField);
      Map<SqlOperator, Supplier<? extends TableFunctionCallImplementor>> tvfMap =
          (Map<SqlOperator, Supplier<? extends TableFunctionCallImplementor>>)
              ReflectionUtils.getFieldValue(rexImpTable, tvfMapField);
      ImmutableMap<SqlOperator, Supplier<? extends TableFunctionCallImplementor>> newTvfMap =
          ImmutableMap.<SqlOperator, Supplier<? extends TableFunctionCallImplementor>>builder()
              .putAll(tvfMap)
              .put(PPLBuiltinOperators.UNCOLLECT_PATTERNS, UncollectPatternsImplementor::new)
              .build();
      ReflectionUtils.setFieldValue(rexImpTable, tvfMapField, newTvfMap);
    }

    void populate() {
      // Register std operator
      registerOperator(AND, SqlStdOperatorTable.AND);
      registerOperator(OR, SqlStdOperatorTable.OR);
      registerOperator(NOT, SqlStdOperatorTable.NOT);
      registerOperator(NOTEQUAL, SqlStdOperatorTable.NOT_EQUALS);
      registerOperator(EQUAL, SqlStdOperatorTable.EQUALS);
      registerOperator(GREATER, SqlStdOperatorTable.GREATER_THAN);
      registerOperator(GTE, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
      registerOperator(LESS, SqlStdOperatorTable.LESS_THAN);
      registerOperator(LTE, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
      registerOperator(ADD, SqlStdOperatorTable.PLUS);
      registerOperator(SUBTRACT, SqlStdOperatorTable.MINUS);
      registerOperator(MULTIPLY, SqlStdOperatorTable.MULTIPLY);
      // registerOperator(DIVIDE, SqlStdOperatorTable.DIVIDE);
      registerOperator(ASCII, SqlStdOperatorTable.ASCII);
      registerOperator(LENGTH, SqlStdOperatorTable.CHAR_LENGTH);
      registerOperator(LOWER, SqlStdOperatorTable.LOWER);
      registerOperator(POSITION, SqlStdOperatorTable.POSITION);
      registerOperator(SUBSTRING, SqlStdOperatorTable.SUBSTRING);
      registerOperator(SUBSTR, SqlStdOperatorTable.SUBSTRING);
      registerOperator(UPPER, SqlStdOperatorTable.UPPER);
      registerOperator(ABS, SqlStdOperatorTable.ABS);
      registerOperator(ACOS, SqlStdOperatorTable.ACOS);
      registerOperator(ASIN, SqlStdOperatorTable.ASIN);
      registerOperator(ATAN, SqlStdOperatorTable.ATAN);
      registerOperator(ATAN2, SqlStdOperatorTable.ATAN2);
      registerOperator(CEILING, SqlStdOperatorTable.CEIL);
      registerOperator(COS, SqlStdOperatorTable.COS);
      registerOperator(COT, SqlStdOperatorTable.COT);
      registerOperator(DEGREES, SqlStdOperatorTable.DEGREES);
      registerOperator(EXP, SqlStdOperatorTable.EXP);
      registerOperator(FLOOR, SqlStdOperatorTable.FLOOR);
      registerOperator(LN, SqlStdOperatorTable.LN);
      registerOperator(LOG10, SqlStdOperatorTable.LOG10);
      registerOperator(PI, SqlStdOperatorTable.PI);
      registerOperator(POW, SqlStdOperatorTable.POWER);
      registerOperator(POWER, SqlStdOperatorTable.POWER);
      registerOperator(RADIANS, SqlStdOperatorTable.RADIANS);
      registerOperator(RAND, SqlStdOperatorTable.RAND);
      registerOperator(ROUND, SqlStdOperatorTable.ROUND);
      registerOperator(SIGN, SqlStdOperatorTable.SIGN);
      registerOperator(SIN, SqlStdOperatorTable.SIN);
      registerOperator(CBRT, SqlStdOperatorTable.CBRT);
      registerOperator(IS_NOT_NULL, SqlStdOperatorTable.IS_NOT_NULL);
      registerOperator(IS_NULL, SqlStdOperatorTable.IS_NULL);

      // Register library operator
      registerOperator(REGEXP, SqlLibraryOperators.REGEXP);
      registerOperator(CONCAT, SqlLibraryOperators.CONCAT_FUNCTION);
      registerOperator(CONCAT_WS, SqlLibraryOperators.CONCAT_WS);
      registerOperator(LIKE, SqlLibraryOperators.ILIKE);
      registerOperator(CONCAT_WS, SqlLibraryOperators.CONCAT_WS);
      registerOperator(REVERSE, SqlLibraryOperators.REVERSE);
      registerOperator(RIGHT, SqlLibraryOperators.RIGHT);
      registerOperator(LEFT, SqlLibraryOperators.LEFT);
      registerOperator(LOG2, SqlLibraryOperators.LOG2);
      registerOperator(INTERNAL_REGEXP_EXTRACT, SqlLibraryOperators.REGEXP_EXTRACT);
      registerOperator(INTERNAL_REGEXP_REPLACE_3, SqlLibraryOperators.REGEXP_REPLACE_3);

      // Register PPL UDF operator
      registerOperator(SPAN, PPLBuiltinOperators.SPAN);
      registerOperator(PATTERN_PARSER, PPLBuiltinOperators.PATTERN_PARSER);

      // Register implementation.
      // Note, make the implementation an individual class if too complex.
      register(
          TRIM,
          ((FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.BOTH),
                      builder.makeLiteral(" "),
                      arg)));
      register(
          LTRIM,
          ((FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.LEADING),
                      builder.makeLiteral(" "),
                      arg)));
      register(
          RTRIM,
          ((FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.TRAILING),
                      builder.makeLiteral(" "),
                      arg)));
      register(
          STRCMP,
          ((FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.STRCMP, arg2, arg1)));
      register(
          LOG,
          ((FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.LOG, arg2, arg1)));
      register(
          LOG,
          ((FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlLibraryOperators.LOG,
                      arg,
                      builder.makeApproxLiteral(BigDecimal.valueOf(E)))));
      register(
          TYPEOF,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeLiteral(getLegacyTypeName(arg.getType(), QueryType.PPL)));
      register(XOR, new XOR_FUNC());

      // Register table function operator
      registerOperator(UNCOLLECT_PATTERNS, PPLBuiltinOperators.UNCOLLECT_PATTERNS);
      registerTvfIntoRexImpTable();
    }
  }

  private static class Builder extends AbstractBuilder {
    private final Map<BuiltinFunctionName, PairList<CalciteFuncSignature, FunctionImp>> map =
        new HashMap<>();

    @Override
    void register(BuiltinFunctionName functionName, FunctionImp implement) {
      CalciteFuncSignature signature =
          new CalciteFuncSignature(functionName.getName(), implement.getParams());
      if (map.containsKey(functionName)) {
        map.get(functionName).add(signature, implement);
      } else {
        map.put(functionName, PairList.of(signature, implement));
      }
    }
  }

  // -------------------------------------------------------------
  //                   FUNCTIONS
  // -------------------------------------------------------------
  /** Implement XOR via NOT_EQUAL, and limit the arguments' type to boolean only */
  private static class XOR_FUNC implements FunctionImp2 {
    @Override
    public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
      return builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, arg1, arg2);
    }

    @Override
    public List<RelDataType> getParams() {
      RelDataType boolType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
      return List.of(boolType, boolType);
    }
  }
}
