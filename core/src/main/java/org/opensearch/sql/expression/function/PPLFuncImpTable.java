/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL;
import static org.apache.calcite.sql.type.SqlTypeFamily.IGNORE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.TransferUserDefinedAggFunction;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ABS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ACOS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.AND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ASCII;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ASIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ATAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ATAN2;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.AVG;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CBRT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CEIL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CEILING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CIDRMATCH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COALESCE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONCAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONCAT_WS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONV;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CONVERT_TZ;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COSH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COUNT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CRC32;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CURDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CURRENT_DATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CURRENT_TIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CURRENT_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CURTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATEDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATETIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATE_ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATE_FORMAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATE_SUB;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAYNAME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAYOFMONTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAYOFWEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAYOFYEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_MONTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DEGREES;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDEFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.E;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EARLIEST;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EXISTS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EXP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EXPM1;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FILTER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FLOOR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FORALL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FROM_DAYS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FROM_UNIXTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GET_FORMAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GREATER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.GTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.HOUR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.HOUR_OF_DAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IFNULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_GROK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_ITEM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_PATTERN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_PATTERN_PARSER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_REGEXP_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_3;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_BLANK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_EMPTY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_PRESENT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_APPEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_ARRAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_DELETE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_KEYS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_OBJECT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_SET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_VALID;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LAST_DAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LATEST;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LEFT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LESS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOCALTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOCALTIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOCATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOG;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOG10;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOG2;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOWER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LTRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAKEDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAKETIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH_BOOL_PREFIX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH_PHRASE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH_PHRASE_PREFIX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MD5;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MICROSECOND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MINUTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MINUTE_OF_DAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MINUTE_OF_HOUR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MOD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MODULUS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MODULUSFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MONTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MONTHNAME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MONTH_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLYFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTI_MATCH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOTEQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOW;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NULLIF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.OR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.PERCENTILE_APPROX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.PERIOD_ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.PERIOD_DIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.PI;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POSITION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POW;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POWER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.QUARTER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.QUERY_STRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RADIANS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RAND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REDUCE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REGEXP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REPLACE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REVERSE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RIGHT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RINT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ROUND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RTRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SECOND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SECOND_OF_MINUTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SEC_TO_TIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SHA1;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SHA2;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SIGN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SIGNUM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SIMPLE_QUERY_STRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SINH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SPAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SQRT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STDDEV_POP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STDDEV_SAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STRCMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STR_TO_DATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBSTR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBSTRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTRACTFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SYSDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TAKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMEDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIME_FORMAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIME_TO_SEC;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TO_DAYS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TO_SECONDS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRANSFORM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRUNCATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TYPEOF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UNIX_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UPPER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UTC_DATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UTC_TIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UTC_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.VARPOP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.VARSAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEKDAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEKOFYEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.XOR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.YEARWEEK;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.udf.udaf.LogPatternAggFunction;
import org.opensearch.sql.calcite.udf.udaf.PercentileApproxFunction;
import org.opensearch.sql.calcite.udf.udaf.TakeAggFunction;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.executor.QueryType;

public class PPLFuncImpTable {
  private static final Logger logger = LogManager.getLogger(PPLFuncImpTable.class);

  /** A lambda function interface which could apply parameters to get AggCall. */
  @FunctionalInterface
  public interface AggHandler {
    RelBuilder.AggCall apply(
        boolean distinct, RexNode field, List<RexNode> argList, CalcitePlanContext context);
  }

  public interface FunctionImp {
    RelDataType ANY_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);

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
    final AggBuilder aggBuilder = new AggBuilder();
    aggBuilder.populate();
    INSTANCE = new PPLFuncImpTable(builder, aggBuilder);
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

  /**
   * The registry for built-in agg functions. Agg Functions defined by the PPL specification, whose
   * implementations are independent of any specific data storage, should be registered here
   * internally.
   */
  private final ImmutableMap<BuiltinFunctionName, AggHandler> aggFunctionRegistry;

  /**
   * The external agg function registry. Agg Functions whose implementations depend on a specific
   * data engine should be registered here. This reduces coupling between the core module and
   * particular storage backends.
   */
  private final Map<BuiltinFunctionName, AggHandler> aggExternalFunctionRegistry;

  private PPLFuncImpTable(Builder builder, AggBuilder aggBuilder) {
    final ImmutableMap.Builder<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>>
        mapBuilder = ImmutableMap.builder();
    builder.map.forEach((k, v) -> mapBuilder.put(k, List.copyOf(v)));
    this.functionRegistry = ImmutableMap.copyOf(mapBuilder.build());
    this.externalFunctionRegistry = new ConcurrentHashMap<>();

    final ImmutableMap.Builder<BuiltinFunctionName, AggHandler> aggMapBuilder =
        ImmutableMap.builder();
    aggBuilder.map.forEach(aggMapBuilder::put);
    this.aggFunctionRegistry = ImmutableMap.copyOf(aggMapBuilder.build());
    this.aggExternalFunctionRegistry = new ConcurrentHashMap<>();
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
    externalFunctionRegistry.compute(
        functionName,
        (name, existingList) -> {
          List<Pair<CalciteFuncSignature, FunctionImp>> list =
              existingList == null ? new ArrayList<>() : new ArrayList<>(existingList);
          list.add(Pair.of(signature, functionImp));
          return list;
        });
  }

  /**
   * Register a function implementation from external services dynamically.
   *
   * @param functionName the name of the function, has to be defined in BuiltinFunctionName
   * @param functionImp the implementation of the agg function
   */
  public void registerExternalAggFunction(
      BuiltinFunctionName functionName, AggHandler functionImp) {
    aggExternalFunctionRegistry.put(functionName, functionImp);
  }

  public RelBuilder.AggCall resolveAgg(
      BuiltinFunctionName functionName,
      boolean distinct,
      RexNode field,
      List<RexNode> argList,
      CalcitePlanContext context) {
    AggHandler handler = aggExternalFunctionRegistry.get(functionName);
    if (handler == null) {
      handler = aggFunctionRegistry.get(functionName);
    }
    if (handler == null) {
      throw new IllegalStateException(String.format("Cannot resolve function: %s", functionName));
    }
    return handler.apply(distinct, field, argList, context);
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

    // Make compulsory casts for some functions that require specific casting of arguments.
    // For example, the REDUCE function requires the second argument to be cast to the
    // return type of the lambda function.
    compulsoryCast(builder, functionName, args);

    List<RelDataType> argTypes = Arrays.stream(args).map(RexNode::getType).toList();
    try {
      for (Map.Entry<CalciteFuncSignature, FunctionImp> implement : implementList) {
        if (implement.getKey().match(functionName.getName(), argTypes)) {
          return implement.getValue().resolve(builder, args);
        }
      }

      // If no implementation found with exact match, try to cast arguments to match the
      // signatures.
      RexNode coerced = resolveWithCoercion(builder, functionName, implementList, args);
      if (coerced != null) {
        return coerced;
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
      String signature = implement.getKey().typeChecker().getAllowedSignatures();
      if (!signature.isEmpty()) {
        allowedSignatures.add(signature);
      }
    }
    throw new ExpressionEvaluationException(
        String.format(
            "%s function expects {%s}, but got %s",
            functionName, allowedSignatures, getActualSignature(argTypes)));
  }

  /**
   * Ad-hoc coercion for some functions that require specific casting of arguments. Now it only
   * applies to the REDUCE function.
   */
  private void compulsoryCast(
      final RexBuilder builder, final BuiltinFunctionName functionName, RexNode... args) {

    //noinspection SwitchStatementWithTooFewBranches
    switch (functionName) {
      case BuiltinFunctionName.REDUCE:
        // Set the second argument to the return type of the lambda function, so that
        // code generated with linq4j can correctly accumulate the result.
        RexLambda call = (RexLambda) args[2];
        args[1] = builder.makeCast(call.getType(), args[1], true, true);
        break;
      default:
        break;
    }
  }

  private @Nullable RexNode resolveWithCoercion(
      final RexBuilder builder,
      final BuiltinFunctionName functionName,
      List<Pair<CalciteFuncSignature, FunctionImp>> implementList,
      RexNode... args) {
    if (BuiltinFunctionName.COMPARATORS.contains(functionName)) {
      for (Map.Entry<CalciteFuncSignature, FunctionImp> implement : implementList) {
        var widenedArgs = CoercionUtils.widenArguments(builder, List.of(args));
        if (widenedArgs != null) {
          boolean matchSignature =
              implement
                  .getKey()
                  .typeChecker()
                  .checkOperandTypes(widenedArgs.stream().map(RexNode::getType).toList());
          if (matchSignature) {
            return implement.getValue().resolve(builder, widenedArgs.toArray(new RexNode[0]));
          }
        }
      }
    } else {
      for (Map.Entry<CalciteFuncSignature, FunctionImp> implement : implementList) {
        var signature = implement.getKey();
        var castedArgs =
            CoercionUtils.castArguments(builder, signature.typeChecker(), List.of(args));
        if (castedArgs != null) {
          // If compatible function is found, replace the original RexNode with cast node
          // TODO: check - this is a return-once-found implementation, rest possible combinations
          //  will be skipped.
          //  Maybe can be improved to return the best match? E.g. convert to timestamp when date,
          //  time, and timestamp are all possible.
          return implement.getValue().resolve(builder, castedArgs.toArray(new RexNode[0]));
        }
      }
    }
    return null;
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

    /**
     * Register one or multiple operators under a single function name. This allows function
     * overloading based on operand types.
     *
     * <p>When a function is called, the system will try each registered operator in sequence,
     * checking if the provided arguments match the operator's type requirements. The first operator
     * whose type checker accepts the arguments will be used to execute the function.
     *
     * @param functionName the built-in function name under which to register the operators
     * @param operators the operators to associate with this function name, tried in sequence until
     *     one matches the argument types during resolution
     */
    public void registerOperator(BuiltinFunctionName functionName, SqlOperator... operators) {
      for (SqlOperator operator : operators) {
        SqlOperandTypeChecker typeChecker;
        if (operator instanceof SqlUserDefinedFunction udfOperator) {
          typeChecker = extractTypeCheckerFromUDF(udfOperator);
        } else {
          typeChecker = operator.getOperandTypeChecker();
        }

        // Only the composite operand type checker for UDFs are concerned here.
        if (operator instanceof SqlUserDefinedFunction
            && typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
          // UDFs implement their own composite type checkers, which always use OR logic for
          // argument
          // types. Verifying the composition type would require accessing a protected field in
          // CompositeOperandTypeChecker. If access to this field is not allowed, type checking will
          // be skipped, so we avoid checking the composition type here.
          register(
              functionName, wrapWithCompositeTypeChecker(operator, compositeTypeChecker, false));
        } else if (typeChecker instanceof ImplicitCastOperandTypeChecker implicitCastTypeChecker) {
          register(
              functionName, wrapWithImplicitCastTypeChecker(operator, implicitCastTypeChecker));
        } else if (typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
          // If compositeTypeChecker contains operand checkers other than family type checkers or
          // other than OR compositions, the function with be registered with a null type checker,
          // which means the function will not be type checked.
          register(
              functionName, wrapWithCompositeTypeChecker(operator, compositeTypeChecker, true));
        } else if (typeChecker instanceof SameOperandTypeChecker comparableTypeChecker) {
          // Comparison operators like EQUAL, GREATER_THAN, LESS_THAN, etc.
          // SameOperandTypeCheckers like COALESCE, IFNULL, etc.
          register(functionName, wrapWithComparableTypeChecker(operator, comparableTypeChecker));
        } else if (typeChecker
            instanceof UDFOperandMetadata.UDTOperandMetadata udtOperandMetadata) {
          register(functionName, wrapWithUdtTypeChecker(operator, udtOperandMetadata));
        } else {
          logger.info(
              "Cannot create type checker for function: {}. Will skip its type checking",
              functionName);
          register(
              functionName,
              (RexBuilder builder, RexNode... node) -> builder.makeCall(operator, node));
        }
      }
    }

    private static SqlOperandTypeChecker extractTypeCheckerFromUDF(
        SqlUserDefinedFunction udfOperator) {
      UDFOperandMetadata udfOperandMetadata =
          (UDFOperandMetadata) udfOperator.getOperandTypeChecker();
      return (udfOperandMetadata == null) ? null : udfOperandMetadata.getInnerTypeChecker();
    }

    // Such wrapWith*TypeChecker methods are useful in that we don't have to create explicit
    // overrides of resolve function for different number of operands.
    // I.e. we don't have to explicitly call
    //  (FuncImp1) (builder, arg1) -> builder.makeCall(operator, arg1);
    // (FuncImp2) (builder, arg1, arg2) -> builder.makeCall(operator, arg1, arg2);
    // etc.

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

    private static FunctionImp wrapWithUdtTypeChecker(
        SqlOperator operator, UDFOperandMetadata.UDTOperandMetadata udtOperandMetadata) {
      return new FunctionImp() {
        @Override
        public RexNode resolve(RexBuilder builder, RexNode... args) {
          return builder.makeCall(operator, args);
        }

        @Override
        public PPLTypeChecker getTypeChecker() {
          return PPLTypeChecker.wrapUDT(udtOperandMetadata.allowedParamTypes());
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

    void populate() {
      // register operators for comparison
      registerOperator(NOTEQUAL, PPLBuiltinOperators.NOT_EQUALS_IP, SqlStdOperatorTable.NOT_EQUALS);
      registerOperator(EQUAL, PPLBuiltinOperators.EQUALS_IP, SqlStdOperatorTable.EQUALS);
      registerOperator(GREATER, PPLBuiltinOperators.GREATER_IP, SqlStdOperatorTable.GREATER_THAN);
      registerOperator(GTE, PPLBuiltinOperators.GTE_IP, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
      registerOperator(LESS, PPLBuiltinOperators.LESS_IP, SqlStdOperatorTable.LESS_THAN);
      registerOperator(LTE, PPLBuiltinOperators.LTE_IP, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

      // Register std operator
      registerOperator(AND, SqlStdOperatorTable.AND);
      registerOperator(OR, SqlStdOperatorTable.OR);
      registerOperator(NOT, SqlStdOperatorTable.NOT);
      registerOperator(ADD, SqlStdOperatorTable.PLUS);
      registerOperator(ADDFUNCTION, SqlStdOperatorTable.PLUS);
      registerOperator(SUBTRACT, SqlStdOperatorTable.MINUS);
      registerOperator(SUBTRACTFUNCTION, SqlStdOperatorTable.MINUS);
      registerOperator(MULTIPLY, SqlStdOperatorTable.MULTIPLY);
      registerOperator(MULTIPLYFUNCTION, SqlStdOperatorTable.MULTIPLY);
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
      registerOperator(SIGNUM, SqlStdOperatorTable.SIGN);
      registerOperator(SIN, SqlStdOperatorTable.SIN);
      registerOperator(CBRT, SqlStdOperatorTable.CBRT);
      registerOperator(IS_NOT_NULL, SqlStdOperatorTable.IS_NOT_NULL);
      registerOperator(IS_PRESENT, SqlStdOperatorTable.IS_NOT_NULL);
      registerOperator(IS_NULL, SqlStdOperatorTable.IS_NULL);
      registerOperator(IFNULL, SqlStdOperatorTable.COALESCE);
      registerOperator(EARLIEST, PPLBuiltinOperators.EARLIEST);
      registerOperator(LATEST, PPLBuiltinOperators.LATEST);
      registerOperator(COALESCE, SqlStdOperatorTable.COALESCE);

      // Register library operator
      registerOperator(REGEXP, SqlLibraryOperators.REGEXP);
      registerOperator(CONCAT, SqlLibraryOperators.CONCAT_FUNCTION);
      registerOperator(CONCAT_WS, SqlLibraryOperators.CONCAT_WS);
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
      registerOperator(COSH, PPLBuiltinOperators.COSH);
      registerOperator(SINH, PPLBuiltinOperators.SINH);
      registerOperator(EXPM1, PPLBuiltinOperators.EXPM1);
      registerOperator(RINT, PPLBuiltinOperators.RINT);
      registerOperator(SPAN, PPLBuiltinOperators.SPAN);
      registerOperator(E, PPLBuiltinOperators.E);
      registerOperator(CONV, PPLBuiltinOperators.CONV);
      registerOperator(MOD, PPLBuiltinOperators.MOD);
      registerOperator(MODULUS, PPLBuiltinOperators.MOD);
      registerOperator(MODULUSFUNCTION, PPLBuiltinOperators.MOD);
      registerOperator(CRC32, PPLBuiltinOperators.CRC32);
      registerOperator(DIVIDE, PPLBuiltinOperators.DIVIDE);
      registerOperator(DIVIDEFUNCTION, PPLBuiltinOperators.DIVIDE);
      registerOperator(SHA2, PPLBuiltinOperators.SHA2);
      registerOperator(CIDRMATCH, PPLBuiltinOperators.CIDRMATCH);
      registerOperator(INTERNAL_GROK, PPLBuiltinOperators.GROK);
      registerOperator(MATCH, PPLBuiltinOperators.MATCH);
      registerOperator(MATCH_PHRASE, PPLBuiltinOperators.MATCH_PHRASE);
      registerOperator(MATCH_BOOL_PREFIX, PPLBuiltinOperators.MATCH_BOOL_PREFIX);
      registerOperator(MATCH_PHRASE_PREFIX, PPLBuiltinOperators.MATCH_PHRASE_PREFIX);
      registerOperator(SIMPLE_QUERY_STRING, PPLBuiltinOperators.SIMPLE_QUERY_STRING);
      registerOperator(QUERY_STRING, PPLBuiltinOperators.QUERY_STRING);
      registerOperator(MULTI_MATCH, PPLBuiltinOperators.MULTI_MATCH);

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

      registerOperator(ARRAY, PPLBuiltinOperators.ARRAY);
      registerOperator(ARRAY_LENGTH, SqlLibraryOperators.ARRAY_LENGTH);
      registerOperator(FORALL, PPLBuiltinOperators.FORALL);
      registerOperator(EXISTS, PPLBuiltinOperators.EXISTS);
      registerOperator(FILTER, PPLBuiltinOperators.FILTER);
      registerOperator(TRANSFORM, PPLBuiltinOperators.TRANSFORM);
      registerOperator(REDUCE, PPLBuiltinOperators.REDUCE);
      // Register Json function
      register(
          JSON_ARRAY,
          ((builder, args) ->
              builder.makeCall(
                  SqlStdOperatorTable.JSON_ARRAY,
                  Stream.concat(Stream.of(builder.makeFlag(NULL_ON_NULL)), Arrays.stream(args))
                      .toArray(RexNode[]::new))));
      register(
          JSON_OBJECT,
          ((builder, args) ->
              builder.makeCall(
                  SqlStdOperatorTable.JSON_OBJECT,
                  Stream.concat(Stream.of(builder.makeFlag(NULL_ON_NULL)), Arrays.stream(args))
                      .toArray(RexNode[]::new))));
      registerOperator(JSON, PPLBuiltinOperators.JSON);
      registerOperator(JSON_ARRAY_LENGTH, PPLBuiltinOperators.JSON_ARRAY_LENGTH);
      registerOperator(JSON_EXTRACT, PPLBuiltinOperators.JSON_EXTRACT);
      registerOperator(JSON_KEYS, PPLBuiltinOperators.JSON_KEYS);
      registerOperator(JSON_VALID, SqlStdOperatorTable.IS_JSON_VALUE);
      registerOperator(JSON_SET, PPLBuiltinOperators.JSON_SET);
      registerOperator(JSON_DELETE, PPLBuiltinOperators.JSON_DELETE);
      registerOperator(JSON_APPEND, PPLBuiltinOperators.JSON_APPEND);
      registerOperator(JSON_EXTEND, PPLBuiltinOperators.JSON_EXTEND);

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
              PPLTypeChecker.family(SqlTypeFamily.CHARACTER)));

      register(
          LTRIM,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.LEADING),
                      builder.makeLiteral(" "),
                      arg),
              PPLTypeChecker.family(SqlTypeFamily.CHARACTER)));
      register(
          RTRIM,
          createFunctionImpWithTypeChecker(
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.TRAILING),
                      builder.makeLiteral(" "),
                      arg),
              PPLTypeChecker.family(SqlTypeFamily.CHARACTER)));
      register(
          ATAN,
          createFunctionImpWithTypeChecker(
              (builder, arg1, arg2) -> builder.makeCall(SqlStdOperatorTable.ATAN2, arg1, arg2),
              PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)));
      register(
          STRCMP,
          createFunctionImpWithTypeChecker(
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.STRCMP, arg2, arg1),
              PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)));
      // SqlStdOperatorTable.SUBSTRING.getOperandTypeChecker is null. We manually create a type
      // checker for it.
      register(
          SUBSTRING,
          wrapWithCompositeTypeChecker(
              SqlStdOperatorTable.SUBSTRING,
              (CompositeOperandTypeChecker)
                  OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER)
                      .or(
                          OperandTypes.family(
                              SqlTypeFamily.CHARACTER,
                              SqlTypeFamily.INTEGER,
                              SqlTypeFamily.INTEGER)),
              false));
      register(
          SUBSTR,
          wrapWithCompositeTypeChecker(
              SqlStdOperatorTable.SUBSTRING,
              (CompositeOperandTypeChecker)
                  OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER)
                      .or(
                          OperandTypes.family(
                              SqlTypeFamily.CHARACTER,
                              SqlTypeFamily.INTEGER,
                              SqlTypeFamily.INTEGER)),
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
      register(
          LIKE,
          createFunctionImpWithTypeChecker(
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlLibraryOperators.ILIKE,
                      arg1,
                      arg2,
                      // TODO: Figure out escaping solution. '\\' is used for JSON input but is not
                      // necessary for SQL function input
                      builder.makeLiteral("\\")),
              PPLTypeChecker.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)));
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

  private static class AggBuilder {
    private final Map<BuiltinFunctionName, AggHandler> map = new HashMap<>();

    void register(BuiltinFunctionName functionName, AggHandler aggHandler) {
      map.put(functionName, aggHandler);
    }

    void populate() {
      register(MAX, (distinct, field, argList, ctx) -> ctx.relBuilder.max(field));
      register(MIN, (distinct, field, argList, ctx) -> ctx.relBuilder.min(field));

      register(AVG, (distinct, field, argList, ctx) -> ctx.relBuilder.avg(distinct, null, field));

      register(
          COUNT,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.count(
                  distinct, null, field == null ? ImmutableList.of() : ImmutableList.of(field)));
      register(SUM, (distinct, field, argList, ctx) -> ctx.relBuilder.sum(distinct, null, field));

      register(
          VARSAMP,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.aggregateCall(VAR_SAMP_NULLABLE, field));

      register(
          VARPOP,
          (distinct, field, argList, ctx) -> ctx.relBuilder.aggregateCall(VAR_POP_NULLABLE, field));

      register(
          STDDEV_SAMP,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.aggregateCall(STDDEV_SAMP_NULLABLE, field));

      register(
          STDDEV_POP,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.aggregateCall(STDDEV_POP_NULLABLE, field));

      register(
          TAKE,
          (distinct, field, argList, ctx) -> {
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            return TransferUserDefinedAggFunction(
                TakeAggFunction.class,
                "TAKE",
                UserDefinedFunctionUtils.getReturnTypeInferenceForArray(),
                List.of(field),
                newArgList,
                ctx.relBuilder);
          });

      register(
          PERCENTILE_APPROX,
          (distinct, field, argList, ctx) -> {
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            newArgList.add(ctx.rexBuilder.makeFlag(field.getType().getSqlTypeName()));
            return TransferUserDefinedAggFunction(
                PercentileApproxFunction.class,
                "percentile_approx",
                ReturnTypes.ARG0_FORCE_NULLABLE,
                List.of(field),
                newArgList,
                ctx.relBuilder);
          });

      register(
          INTERNAL_PATTERN,
          (distinct, field, argList, ctx) ->
              TransferUserDefinedAggFunction(
                  LogPatternAggFunction.class,
                  "pattern",
                  ReturnTypes.explicit(UserDefinedFunctionUtils.nullablePatternAggList),
                  List.of(field),
                  argList,
                  ctx.relBuilder));
    }
  }
}
