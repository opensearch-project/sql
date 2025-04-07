/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static java.lang.Math.E;
import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EXP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FLOOR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GREATER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GTE;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UPPER;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

public class PPLFuncImpTable {

  public interface FunctionImp {
    RelDataType ANY_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);

    RexNode apply(RexBuilder builder, RexNode... args);

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

    RexNode apply(RexBuilder builder, RexNode arg1);

    @Override
    default RexNode apply(RexBuilder builder, RexNode... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("This function requires exactly 1 arguments");
      }
      return apply(builder, args[0]);
    }

    @Override
    default List<RelDataType> getParams() {
      return ANY_TYPE_1;
    }
  }

  public interface FunctionImp2 extends FunctionImp {
    List<RelDataType> ANY_TYPE_2 = List.of(ANY_TYPE, ANY_TYPE);

    RexNode apply(RexBuilder builder, RexNode arg1, RexNode arg2);

    @Override
    default RexNode apply(RexBuilder builder, RexNode... args) {
      if (args.length != 2) {
        throw new IllegalArgumentException("This function requires exactly 2 arguments");
      }
      return apply(builder, args[0], args[1]);
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

  public @Nullable FunctionImp get(final BuiltinFunctionName functionName) {
    final PairList<CalciteFuncSignature, FunctionImp> implementList =
        requireNonNull(map.get(functionName));
    if (implementList.isEmpty()) {
      throw new NullPointerException();
    }
    return implementList.getFirst().getValue();
  }

  public @Nullable FunctionImp resolveSafe(final String functionName, List<RexNode> args) {
    try {
      return resolve(functionName, args);
    } catch (IllegalStateException e) {
      return null;
    }
  }

  public FunctionImp resolve(final String functionName, List<RexNode> args) {
    Optional<BuiltinFunctionName> funcNameOpt = BuiltinFunctionName.of(functionName);
    if (funcNameOpt.isEmpty()) {
      throw new IllegalArgumentException(String.format("Unsupported function: %s", functionName));
    }
    BuiltinFunctionName.of(functionName);
    return resolve(funcNameOpt.get(), args);
  }

  public FunctionImp resolve(final BuiltinFunctionName functionName, List<RexNode> args) {
    final PairList<CalciteFuncSignature, FunctionImp> implementList = map.get(functionName);
    if (implementList == null || implementList.isEmpty()) {
      throw new IllegalStateException(String.format("Cannot resolve function: %s", functionName));
    }
    List<RelDataType> argTypes = args.stream().map(RexNode::getType).toList();
    for (Map.Entry<CalciteFuncSignature, FunctionImp> implement : implementList) {
      if (implement.getKey().match(functionName.getName(), argTypes)) {
        return implement.getValue();
      }
    }
    throw new IllegalStateException(
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

    void populate() {
      // Register std operator
      registerOperator(AND, SqlStdOperatorTable.AND);
      registerOperator(OR, SqlStdOperatorTable.OR);
      registerOperator(NOT, SqlStdOperatorTable.NOT);
      // registerOperator(XOR, SqlStdOperatorTable.NOT_EQUALS); Migrate from BuiltinFunctionUtils
      // while seems not right
      registerOperator(NOTEQUAL, SqlStdOperatorTable.NOT_EQUALS);
      registerOperator(EQUAL, SqlStdOperatorTable.EQUALS);
      registerOperator(GREATER, SqlStdOperatorTable.GREATER_THAN);
      registerOperator(GTE, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
      registerOperator(LESS, SqlStdOperatorTable.LESS_THAN);
      registerOperator(LTE, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
      registerOperator(ADD, SqlStdOperatorTable.PLUS);
      registerOperator(SUBTRACT, SqlStdOperatorTable.MINUS);
      registerOperator(MULTIPLY, SqlStdOperatorTable.MULTIPLY);
      registerOperator(DIVIDE, SqlStdOperatorTable.DIVIDE);
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

      // Register PPL UDF operator
      registerOperator(SPAN, PPLBuiltinOperators.SPAN);

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
}
