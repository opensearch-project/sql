/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.apache.calcite.sql.type.SqlTypeFamily.IGNORE;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.*;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.function.Supplier;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.TableFunctionCallImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.SqlUncollectPatternsTableFunction.UncollectPatternsImplementor;
import org.opensearch.sql.utils.ReflectionUtils;

public class PPLFuncImpTable {
  private static final Logger logger = LogManager.getLogger(PPLFuncImpTable.class);

  public interface FunctionImp {
    RexNode resolve(RexBuilder builder, RexNode... args);

    /**
     * @return the PPLTypeChecker. Default return null implies unknown parameters {@link
     *     CalciteFuncSignature} won't check parameters if it's null
     */
    default PPLTypeChecker getTypeChecker() {
      return null;
    }
  }

  public interface FunctionImp1 extends FunctionImp {
    RexNode resolve(RexBuilder builder, RexNode arg1);

    PPLTypeChecker IGNORE_1 = PPLTypeChecker.family(IGNORE);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("This function requires exactly 1 arguments");
      }
      return resolve(builder, args[0]);
    }

    @Override
    default PPLTypeChecker getTypeChecker() {
      return IGNORE_1;
    }
  }

  public interface FunctionImp2 extends FunctionImp {
    PPLTypeChecker IGNORE_2 = PPLTypeChecker.family(IGNORE, IGNORE);

    RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 2) {
        throw new IllegalArgumentException("This function requires exactly 2 arguments");
      }
      return resolve(builder, args[0], args[1]);
    }

    @Override
    default PPLTypeChecker getTypeChecker() {
      return IGNORE_2;
    }
  }

  /** The singleton instance. */
  public static final PPLFuncImpTable INSTANCE;

  static {
    final Builder builder = new Builder();
    builder.populate();
    INSTANCE = new PPLFuncImpTable(builder);
  }

  /**
   * The registry for built-in functions. Functions defined by the PPL specification, whose
   * implementations are independent of any specific data storage, should be registered here
   * internally.
   */
  private final ImmutableMap<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>>
      functionRegistry;

  /**
   * The external function registry. Functions whose implementations depend on a specific data
   * engine should be registered here. This reduces coupling between the core module and particular
   * storage backends.
   */
  private final Map<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>>
      externalFunctionRegistry;

  private PPLFuncImpTable(Builder builder) {
    final ImmutableMap.Builder<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>>
        mapBuilder = ImmutableMap.builder();
    builder.map.forEach((k, v) -> mapBuilder.put(k, List.copyOf(v)));
    this.functionRegistry = ImmutableMap.copyOf(mapBuilder.build());
    this.externalFunctionRegistry = new HashMap<>();
  }

  /**
   * Register a function implementation from external services dynamically.
   *
   * @param functionName the name of the function, has to be defined in BuiltinFunctionName
   * @param functionImp the implementation of the function
   */
  public void registerExternalFunction(BuiltinFunctionName functionName, FunctionImp functionImp) {
    CalciteFuncSignature signature =
        new CalciteFuncSignature(functionName.getName(), functionImp.getTypeChecker());
    if (externalFunctionRegistry.containsKey(functionName)) {
      externalFunctionRegistry.get(functionName).add(Pair.of(signature, functionImp));
    } else {
      externalFunctionRegistry.put(
          functionName, new ArrayList<>(List.of(Pair.of(signature, functionImp))));
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
    // Check the external function registry first. This allows the data-storage-dependent
    // function implementations to override the internal ones with the same name.
    List<Pair<CalciteFuncSignature, FunctionImp>> implementList =
        externalFunctionRegistry.get(functionName);
    // If the function is not part of the external registry, check the internal registry.
    if (implementList == null) {
      implementList = functionRegistry.get(functionName);
    }
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
      throw new ExpressionEvaluationException(
          String.format(
              "Cannot resolve function: %s, arguments: %s, caused by: %s",
              functionName, getActualSignature(argTypes), e.getMessage()),
          e);
    }
    StringJoiner allowedSignatures = new StringJoiner(",");
    for (var implement : implementList) {
      allowedSignatures.add(implement.getKey().typeChecker().getAllowedSignatures());
    }
    throw new ExpressionEvaluationException(
        String.format(
            "%s function expects {%s}, but got %s",
            functionName, allowedSignatures, getActualSignature(argTypes)));
  }

  private static String getActualSignature(List<RelDataType> argTypes) {
    return "["
        + argTypes.stream()
            .map(OpenSearchTypeFactory::convertRelDataTypeToExprType)
            .map(Objects::toString)
            .collect(Collectors.joining(","))
        + "]";
  }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private abstract static class AbstractBuilder {

    /** Maps an operator to an implementation. */
    abstract void register(BuiltinFunctionName functionName, FunctionImp functionImp);

    void registerOperator(BuiltinFunctionName functionName, SqlOperator operator) {
      SqlOperandTypeChecker typeChecker;
      if (operator instanceof SqlUserDefinedFunction udfOperator) {
        typeChecker = extractTypeCheckerFromUDF(udfOperator);
      } else {
        typeChecker = operator.getOperandTypeChecker();
      }

      // Only the composite operand type checker for UDFs are concerned here.
      if (operator instanceof SqlUserDefinedFunction
          && typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
        // UDFs implement their own composite type checkers, which always use OR logic for argument
        // types. Verifying the composition type would require accessing a protected field in
        // CompositeOperandTypeChecker. If access to this field is not allowed, type checking will
        // be skipped, so we avoid checking the composition type here.
        register(functionName, wrapWithCompositeTypeChecker(operator, compositeTypeChecker, false));
      } else if (typeChecker instanceof ImplicitCastOperandTypeChecker implicitCastTypeChecker) {
        register(functionName, wrapWithImplicitCastTypeChecker(operator, implicitCastTypeChecker));
      } else if (typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
        // If compositeTypeChecker contains operand checkers other than family type checkers or
        // other than OR compositions, the function with be registered with a null type checker,
        // which means the function will not be type checked.
        register(functionName, wrapWithCompositeTypeChecker(operator, compositeTypeChecker, true));
      } else if (typeChecker instanceof SameOperandTypeChecker comparableTypeChecker) {
        // Comparison operators like EQUAL, GREATER_THAN, LESS_THAN, etc.
        // SameOperandTypeCheckers like COALESCE, IFNULL, etc.
        register(functionName, wrapWithComparableTypeChecker(operator, comparableTypeChecker));
      } else {
        logger.info(
            "Cannot create type checker for function: {}. Will skip its type checking",
            functionName);
        register(
            functionName,
            (RexBuilder builder, RexNode... node) -> builder.makeCall(operator, node));
      }
    }

    private static SqlOperandTypeChecker extractTypeCheckerFromUDF(
        SqlUserDefinedFunction udfOperator) {
      UDFOperandMetadata udfOperandMetadata =
          (UDFOperandMetadata) udfOperator.getOperandTypeChecker();
      return (udfOperandMetadata == null) ? null : udfOperandMetadata.getInnerTypeChecker();
    }

    /**
     * Wrap a SqlOperator into a FunctionImp with a composite type checker.
     *
     * @param operator the SqlOperator to wrap
     * @param typeChecker the CompositeOperandTypeChecker to use for type checking
     * @param checkCompositionType if true, the type checker will check whether the composition type
     *     of the type checker is OR.
     * @return a FunctionImp that resolves to the operator and has the specified type checker
     */
    private static FunctionImp wrapWithCompositeTypeChecker(
        SqlOperator operator,
        CompositeOperandTypeChecker typeChecker,
        boolean checkCompositionType) {
      return new FunctionImp() {
        @Override
        public RexNode resolve(RexBuilder builder, RexNode... args) {
          return builder.makeCall(operator, args);
        }

        @Override
        public PPLTypeChecker getTypeChecker() {
          try {
            return PPLTypeChecker.wrapComposite(typeChecker, checkCompositionType);
          } catch (IllegalArgumentException | UnsupportedOperationException e) {
            logger.debug(
                String.format(
                    "Failed to create composite type checker for operator: %s. Will skip its type"
                        + " checking",
                    operator.getName()),
                e);
            return null;
          }
        }
      };
    }

    private static FunctionImp wrapWithImplicitCastTypeChecker(
        SqlOperator operator, ImplicitCastOperandTypeChecker typeChecker) {
      return new FunctionImp() {
        @Override
        public RexNode resolve(RexBuilder builder, RexNode... args) {
          return builder.makeCall(operator, args);
        }

        @Override
        public PPLTypeChecker getTypeChecker() {
          return PPLTypeChecker.wrapFamily(typeChecker);
        }
      };
    }

    private static FunctionImp wrapWithComparableTypeChecker(
        SqlOperator operator, SameOperandTypeChecker typeChecker) {
      return new FunctionImp() {
        @Override
        public RexNode resolve(RexBuilder builder, RexNode... args) {
          return builder.makeCall(operator, args);
        }

        @Override
        public PPLTypeChecker getTypeChecker() {
          return PPLTypeChecker.wrapComparable(typeChecker);
        }
      };
    }

    private static FunctionImp createFunctionImpWithTypeChecker(
        BiFunction<RexBuilder, RexNode, RexNode> resolver, PPLTypeChecker typeChecker) {
      return new FunctionImp1() {
        @Override
        public RexNode resolve(RexBuilder builder, RexNode arg1) {
          return resolver.apply(builder, arg1);
        }

        @Override
        public PPLTypeChecker getTypeChecker() {
          return typeChecker;
        }
      };
    }

    private static FunctionImp createFunctionImpWithTypeChecker(
        TriFunction<RexBuilder, RexNode, RexNode, RexNode> resolver, PPLTypeChecker typeChecker) {
      return new FunctionImp2() {
        @Override
        public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
          return resolver.apply(builder, arg1, arg2);
        }

        @Override
        public PPLTypeChecker getTypeChecker() {
          return typeChecker;
        }
      };
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
      registerOperator(TRUNCATE, SqlStdOperatorTable.TRUNCATE);
      registerOperator(ASCII, SqlStdOperatorTable.ASCII);
      registerOperator(LENGTH, SqlStdOperatorTable.CHAR_LENGTH);
      registerOperator(LOWER, SqlStdOperatorTable.LOWER);
      registerOperator(POSITION, SqlStdOperatorTable.POSITION);
      registerOperator(LOCATE, SqlStdOperatorTable.POSITION);
      registerOperator(REPLACE, SqlStdOperatorTable.REPLACE);
      registerOperator(UPPER, SqlStdOperatorTable.UPPER);
      registerOperator(ABS, SqlStdOperatorTable.ABS);
      registerOperator(ACOS, SqlStdOperatorTable.ACOS);
      registerOperator(ASIN, SqlStdOperatorTable.ASIN);
      registerOperator(ATAN, SqlStdOperatorTable.ATAN);
      registerOperator(ATAN2, SqlStdOperatorTable.ATAN2);
      registerOperator(CEIL, SqlStdOperatorTable.CEIL);
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
      registerOperator(IS_PRESENT, SqlStdOperatorTable.IS_NOT_NULL);
      registerOperator(IS_NULL, SqlStdOperatorTable.IS_NULL);
      registerOperator(IFNULL, SqlStdOperatorTable.COALESCE);
      registerOperator(COALESCE, SqlStdOperatorTable.COALESCE);

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
      registerOperator(MD5, SqlLibraryOperators.MD5);
      registerOperator(SHA1, SqlLibraryOperators.SHA1);
      registerOperator(INTERNAL_REGEXP_EXTRACT, SqlLibraryOperators.REGEXP_EXTRACT);
      registerOperator(INTERNAL_REGEXP_REPLACE_3, SqlLibraryOperators.REGEXP_REPLACE_3);

      // Register PPL UDF operator
      registerOperator(SPAN, PPLBuiltinOperators.SPAN);
      registerOperator(E, PPLBuiltinOperators.E);
      registerOperator(CONV, PPLBuiltinOperators.CONV);
      registerOperator(MOD, PPLBuiltinOperators.MOD);
      registerOperator(MODULUS, PPLBuiltinOperators.MOD);
      registerOperator(MODULUSFUNCTION, PPLBuiltinOperators.MOD);
      registerOperator(CRC32, PPLBuiltinOperators.CRC32);
      registerOperator(DIVIDE, PPLBuiltinOperators.DIVIDE);
      registerOperator(SHA2, PPLBuiltinOperators.SHA2);
      registerOperator(CIDRMATCH, PPLBuiltinOperators.CIDRMATCH);
      registerOperator(INTERNAL_GROK, PPLBuiltinOperators.GROK);

      // Register PPL Datetime UDF operator
      registerOperator(TIMESTAMP, PPLBuiltinOperators.TIMESTAMP);
      registerOperator(DATE, PPLBuiltinOperators.DATE);
      registerOperator(TIME, PPLBuiltinOperators.TIME);
      registerOperator(UTC_TIME, PPLBuiltinOperators.UTC_TIME);
      registerOperator(UTC_DATE, PPLBuiltinOperators.UTC_DATE);
      registerOperator(UTC_TIMESTAMP, PPLBuiltinOperators.UTC_TIMESTAMP);
      registerOperator(YEAR, PPLBuiltinOperators.YEAR);
      registerOperator(YEARWEEK, PPLBuiltinOperators.YEARWEEK);
      registerOperator(WEEKDAY, PPLBuiltinOperators.WEEKDAY);
      registerOperator(UNIX_TIMESTAMP, PPLBuiltinOperators.UNIX_TIMESTAMP);
      registerOperator(TO_SECONDS, PPLBuiltinOperators.TO_SECONDS);
      registerOperator(TO_DAYS, PPLBuiltinOperators.TO_DAYS);
      registerOperator(ADDTIME, PPLBuiltinOperators.ADDTIME);
      registerOperator(SUBTIME, PPLBuiltinOperators.SUBTIME);
      registerOperator(ADDDATE, PPLBuiltinOperators.ADDDATE);
      registerOperator(SUBDATE, PPLBuiltinOperators.SUBDATE);
      registerOperator(DATE_ADD, PPLBuiltinOperators.DATE_ADD);
      registerOperator(DATE_SUB, PPLBuiltinOperators.DATE_SUB);
      registerOperator(EXTRACT, PPLBuiltinOperators.EXTRACT);
      registerOperator(QUARTER, PPLBuiltinOperators.QUARTER);
      registerOperator(MONTH, PPLBuiltinOperators.MONTH);
      registerOperator(MONTH_OF_YEAR, PPLBuiltinOperators.MONTH);
      registerOperator(DAY, PPLBuiltinOperators.DAY);
      registerOperator(DAYOFMONTH, PPLBuiltinOperators.DAY);
      registerOperator(DAY_OF_MONTH, PPLBuiltinOperators.DAY);
      registerOperator(DAYOFWEEK, PPLBuiltinOperators.DAY_OF_WEEK);
      registerOperator(DAY_OF_WEEK, PPLBuiltinOperators.DAY_OF_WEEK);
      registerOperator(DAYOFYEAR, PPLBuiltinOperators.DAY_OF_YEAR);
      registerOperator(DAY_OF_YEAR, PPLBuiltinOperators.DAY_OF_YEAR);
      registerOperator(HOUR, PPLBuiltinOperators.HOUR);
      registerOperator(HOUR_OF_DAY, PPLBuiltinOperators.HOUR);
      registerOperator(MINUTE, PPLBuiltinOperators.MINUTE);
      registerOperator(MINUTE_OF_HOUR, PPLBuiltinOperators.MINUTE);
      registerOperator(MINUTE_OF_DAY, PPLBuiltinOperators.MINUTE_OF_DAY);
      registerOperator(SECOND, PPLBuiltinOperators.SECOND);
      registerOperator(SECOND_OF_MINUTE, PPLBuiltinOperators.SECOND);
      registerOperator(MICROSECOND, PPLBuiltinOperators.MICROSECOND);
      registerOperator(CURRENT_TIMESTAMP, PPLBuiltinOperators.NOW);
      registerOperator(NOW, PPLBuiltinOperators.NOW);
      registerOperator(LOCALTIMESTAMP, PPLBuiltinOperators.NOW);
      registerOperator(LOCALTIME, PPLBuiltinOperators.NOW);
      registerOperator(CURTIME, PPLBuiltinOperators.CURRENT_TIME);
      registerOperator(CURRENT_TIME, PPLBuiltinOperators.CURRENT_TIME);
      registerOperator(CURRENT_DATE, PPLBuiltinOperators.CURRENT_DATE);
      registerOperator(CURDATE, PPLBuiltinOperators.CURRENT_DATE);
      registerOperator(DATE_FORMAT, PPLBuiltinOperators.DATE_FORMAT);
      registerOperator(TIME_FORMAT, PPLBuiltinOperators.TIME_FORMAT);
      registerOperator(DAYNAME, PPLBuiltinOperators.DAYNAME);
      registerOperator(MONTHNAME, PPLBuiltinOperators.MONTHNAME);
      registerOperator(CONVERT_TZ, PPLBuiltinOperators.CONVERT_TZ);
      registerOperator(DATEDIFF, PPLBuiltinOperators.DATEDIFF);
      registerOperator(DATETIME, PPLBuiltinOperators.DATETIME);
      registerOperator(TIMESTAMPDIFF, PPLBuiltinOperators.TIMESTAMPDIFF);
      registerOperator(LAST_DAY, PPLBuiltinOperators.LAST_DAY);
      registerOperator(FROM_DAYS, PPLBuiltinOperators.FROM_DAYS);
      registerOperator(FROM_UNIXTIME, PPLBuiltinOperators.FROM_UNIXTIME);
      registerOperator(GET_FORMAT, PPLBuiltinOperators.GET_FORMAT);
      registerOperator(MAKEDATE, PPLBuiltinOperators.MAKEDATE);
      registerOperator(MAKETIME, PPLBuiltinOperators.MAKETIME);
      registerOperator(PERIOD_ADD, PPLBuiltinOperators.PERIOD_ADD);
      registerOperator(PERIOD_DIFF, PPLBuiltinOperators.PERIOD_DIFF);
      registerOperator(SEC_TO_TIME, PPLBuiltinOperators.SEC_TO_TIME);
      registerOperator(STR_TO_DATE, PPLBuiltinOperators.STR_TO_DATE);
      registerOperator(SYSDATE, PPLBuiltinOperators.SYSDATE);
      registerOperator(TIME_TO_SEC, PPLBuiltinOperators.TIME_TO_SEC);
      registerOperator(TIMEDIFF, PPLBuiltinOperators.TIMEDIFF);
      registerOperator(TIMESTAMPADD, PPLBuiltinOperators.TIMESTAMPADD);
      registerOperator(WEEK, PPLBuiltinOperators.WEEK);
      registerOperator(WEEK_OF_YEAR, PPLBuiltinOperators.WEEK);
      registerOperator(WEEKOFYEAR, PPLBuiltinOperators.WEEK);
      registerOperator(INTERNAL_PATTERN_PARSER, PPLBuiltinOperators.PATTERN_PARSER);

      // Register implementation.
      // Note, make the implementation an individual class if too complex.
      register(
          TRIM,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.BOTH),
                      builder.makeLiteral(" "),
                      arg),
              PPLTypeChecker.family(SqlTypeFamily.STRING)));

      register(
          LTRIM,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.LEADING),
                      builder.makeLiteral(" "),
                      arg),
              PPLTypeChecker.family(SqlTypeFamily.STRING)));
      register(
          RTRIM,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.TRAILING),
                      builder.makeLiteral(" "),
                      arg),
              PPLTypeChecker.family(SqlTypeFamily.STRING)));
      register(
          STRCMP,
          createFunctionImpWithTypeChecker(
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.STRCMP, arg2, arg1),
              PPLTypeChecker.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)));
      // SqlStdOperatorTable.SUBSTRING.getOperandTypeChecker is null. We manually create a type
      // checker for it.
      register(
          SUBSTRING,
          wrapWithCompositeTypeChecker(
              SqlStdOperatorTable.SUBSTRING,
              (CompositeOperandTypeChecker)
                  OperandTypes.STRING_INTEGER.or(OperandTypes.STRING_INTEGER_INTEGER),
              false));
      register(
          SUBSTR,
          wrapWithCompositeTypeChecker(
              SqlStdOperatorTable.SUBSTRING,
              (CompositeOperandTypeChecker)
                  OperandTypes.STRING_INTEGER.or(OperandTypes.STRING_INTEGER_INTEGER),
              false));
      // SqlStdOperatorTable.ITEM.getOperandTypeChecker() checks only the first operand instead of
      // all operands. Therefore, we wrap it with a custom CompositeOperandTypeChecker to check both
      // operands.
      register(
          INTERNAL_ITEM,
          wrapWithCompositeTypeChecker(
              SqlStdOperatorTable.ITEM,
              (CompositeOperandTypeChecker)
                  OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER)
                      .or(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.ANY)),
              false));
      register(
          LOG,
          createFunctionImpWithTypeChecker(
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.LOG, arg2, arg1),
              PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)));
      register(
          LOG,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlLibraryOperators.LOG,
                      arg,
                      builder.makeApproxLiteral(BigDecimal.valueOf(Math.E))),
              PPLTypeChecker.family(SqlTypeFamily.NUMERIC)));
      // SqlStdOperatorTable.SQRT is declared but not implemented. The call to SQRT in Calcite is
      // converted to POWER(x, 0.5).
      register(
          SQRT,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.POWER,
                      arg,
                      builder.makeApproxLiteral(BigDecimal.valueOf(0.5))),
              PPLTypeChecker.family(SqlTypeFamily.NUMERIC)));
      register(
          TYPEOF,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeLiteral(getLegacyTypeName(arg.getType(), QueryType.PPL)));
      register(XOR, new XOR_FUNC());
      // SqlStdOperatorTable.CASE.getOperandTypeChecker is null. We manually create a type checker
      // for it. The second and third operands are required to be of the same type. If not,
      // it will throw an IllegalArgumentException with information Can't find leastRestrictive type
      register(
          IF,
          wrapWithImplicitCastTypeChecker(
              SqlStdOperatorTable.CASE,
              OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY)));
      register(
          NULLIF,
          createFunctionImpWithTypeChecker(
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlStdOperatorTable.CASE,
                      builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, arg2),
                      builder.makeNullLiteral(arg1.getType()),
                      arg1),
              PPLTypeChecker.wrapComparable((SameOperandTypeChecker) OperandTypes.SAME_SAME)));
      register(
          IS_EMPTY,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.OR,
                      builder.makeCall(SqlStdOperatorTable.IS_NULL, arg),
                      builder.makeCall(SqlStdOperatorTable.IS_EMPTY, arg)),
              PPLTypeChecker.family(SqlTypeFamily.ANY)));
      register(
          IS_BLANK,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.OR,
                      builder.makeCall(SqlStdOperatorTable.IS_NULL, arg),
                      builder.makeCall(
                          SqlStdOperatorTable.IS_EMPTY,
                          builder.makeCall(
                              SqlStdOperatorTable.TRIM,
                              builder.makeFlag(Flag.BOTH),
                              builder.makeLiteral(" "),
                              arg))),
              PPLTypeChecker.family(SqlTypeFamily.ANY)));

      // Register table function operator
      registerOperator(INTERNAL_UNCOLLECT_PATTERNS, PPLBuiltinOperators.UNCOLLECT_PATTERNS);
      registerTvfIntoRexImpTable();
    }
  }

  private static class Builder extends AbstractBuilder {
    private final Map<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>> map =
        new HashMap<>();

    @Override
    void register(BuiltinFunctionName functionName, FunctionImp implement) {
      CalciteFuncSignature signature =
          new CalciteFuncSignature(functionName.getName(), implement.getTypeChecker());
      if (map.containsKey(functionName)) {
        map.get(functionName).add(Pair.of(signature, implement));
      } else {
        map.put(functionName, new ArrayList<>(List.of(Pair.of(signature, implement))));
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
    public PPLTypeChecker getTypeChecker() {
      SqlTypeFamily booleanFamily = SqlTypeName.BOOLEAN.getFamily();
      return PPLTypeChecker.family(booleanFamily, booleanFamily);
    }
  }
}
