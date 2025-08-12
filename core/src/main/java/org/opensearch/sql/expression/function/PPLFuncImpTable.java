/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.createAggregateFunction;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LIST;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.VALUES;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
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
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.udf.udaf.ListAggFunction;
import org.opensearch.sql.calcite.udf.udaf.LogPatternAggFunction;
import org.opensearch.sql.calcite.udf.udaf.PercentileApproxFunction;
import org.opensearch.sql.calcite.udf.udaf.TakeAggFunction;
import org.opensearch.sql.calcite.udf.udaf.ValuesAggFunction;
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
  }

  public interface FunctionImp1 extends FunctionImp {
    RexNode resolve(RexBuilder builder, RexNode arg1);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("This function requires exactly 1 arguments");
      }
      return resolve(builder, args[0]);
    }
  }

  public interface FunctionImp2 extends FunctionImp {
    RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 2) {
        throw new IllegalArgumentException("This function requires exactly 2 arguments");
      }
      return resolve(builder, args[0], args[1]);
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
  private final ImmutableMap<BuiltinFunctionName, Pair<CalciteFuncSignature, AggHandler>>
      aggFunctionRegistry;

  /**
   * The external agg function registry. Agg Functions whose implementations depend on a specific
   * data engine should be registered here. This reduces coupling between the core module and
   * particular storage backends.
   */
  private final Map<BuiltinFunctionName, Pair<CalciteFuncSignature, AggHandler>>
      aggExternalFunctionRegistry;

  private PPLFuncImpTable(Builder builder, AggBuilder aggBuilder) {
    final ImmutableMap.Builder<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>>
        mapBuilder = ImmutableMap.builder();
    builder.map.forEach((k, v) -> mapBuilder.put(k, List.copyOf(v)));
    this.functionRegistry = ImmutableMap.copyOf(mapBuilder.build());
    this.externalFunctionRegistry = new ConcurrentHashMap<>();

    final ImmutableMap.Builder<BuiltinFunctionName, Pair<CalciteFuncSignature, AggHandler>>
        aggMapBuilder = ImmutableMap.builder();
    aggBuilder.map.forEach(aggMapBuilder::put);
    this.aggFunctionRegistry = ImmutableMap.copyOf(aggMapBuilder.build());
    this.aggExternalFunctionRegistry = new ConcurrentHashMap<>();
  }

  /**
   * Register an operator from external services dynamically.
   *
   * @param functionName the name of the function, has to be defined in BuiltinFunctionName
   * @param operator a SqlOperator representing an externally implemented function
   */
  public void registerExternalOperator(BuiltinFunctionName functionName, SqlOperator operator) {
    PPLTypeChecker typeChecker =
        wrapSqlOperandTypeChecker(
            operator.getOperandTypeChecker(),
            functionName.name(),
            operator instanceof SqlUserDefinedFunction);
    CalciteFuncSignature signature = new CalciteFuncSignature(functionName.getName(), typeChecker);
    externalFunctionRegistry.compute(
        functionName,
        (name, existingList) -> {
          List<Pair<CalciteFuncSignature, FunctionImp>> list =
              existingList == null ? new ArrayList<>() : new ArrayList<>(existingList);
          list.add(Pair.of(signature, (builder, args) -> builder.makeCall(operator, args)));
          return list;
        });
  }

  /**
   * Register an external aggregate operator dynamically.
   *
   * @param functionName the name of the function, has to be defined in BuiltinFunctionName
   * @param aggFunction a SqlUserDefinedAggFunction representing the aggregate function
   *     implementation
   */
  public void registerExternalAggOperator(
      BuiltinFunctionName functionName, SqlUserDefinedAggFunction aggFunction) {
    PPLTypeChecker typeChecker =
        wrapSqlOperandTypeChecker(aggFunction.getOperandTypeChecker(), functionName.name(), true);
    CalciteFuncSignature signature = new CalciteFuncSignature(functionName.getName(), typeChecker);
    AggHandler handler =
        (distinct, field, argList, ctx) ->
            UserDefinedFunctionUtils.makeAggregateCall(
                aggFunction, List.of(field), argList, ctx.relBuilder);
    aggExternalFunctionRegistry.put(functionName, Pair.of(signature, handler));
  }

  public RelBuilder.AggCall resolveAgg(
      BuiltinFunctionName functionName,
      boolean distinct,
      RexNode field,
      List<RexNode> argList,
      CalcitePlanContext context) {
    var implementation = aggExternalFunctionRegistry.get(functionName);
    if (implementation == null) {
      implementation = aggFunctionRegistry.get(functionName);
    }
    if (implementation == null) {
      throw new IllegalStateException(String.format("Cannot resolve function: %s", functionName));
    }
    CalciteFuncSignature signature = implementation.getKey();
    List<RelDataType> argTypes = new ArrayList<>();
    if (field != null) {
      argTypes.add(field.getType());
    }
    // Currently only PERCENTILE_APPROX and TAKE have additional arguments.
    // Their additional arguments will always come as a map of <argName, value>
    List<RelDataType> additionalArgTypes =
        argList.stream().map(PlanUtils::derefMapCall).map(RexNode::getType).toList();
    argTypes.addAll(additionalArgTypes);
    if (!signature.match(functionName.getName(), argTypes)) {
      String errorMessagePattern =
          argTypes.size() <= 1
              ? "Aggregation function %s expects field type {%s}, but got %s"
              : "Aggregation function %s expects field type and additional arguments {%s}, but got"
                  + " %s";
      throw new ExpressionEvaluationException(
          String.format(
              errorMessagePattern,
              functionName,
              signature.typeChecker().getAllowedSignatures(),
              getActualSignature(argTypes)));
    }
    var handler = implementation.getValue();
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

  /**
   * Get a string representation of the argument types expressed in ExprType for error messages.
   *
   * @param argTypes the list of argument types as {@link RelDataType}
   * @return a string in the format [type1,type2,...] representing the argument types
   */
  private static String getActualSignature(List<RelDataType> argTypes) {
    return "["
        + argTypes.stream()
            .map(OpenSearchTypeFactory::convertRelDataTypeToExprType)
            .map(Objects::toString)
            .collect(Collectors.joining(","))
        + "]";
  }

  /**
   * Wraps a {@link SqlOperandTypeChecker} into a {@link PPLTypeChecker} for use in function
   * signature validation.
   *
   * @param typeChecker the original SQL operand type checker
   * @param functionName the name of the function for error reporting
   * @param isUserDefinedFunction true if the function is user-defined, false otherwise
   * @return a {@link PPLTypeChecker} that delegates to the provided {@code typeChecker}
   */
  private static PPLTypeChecker wrapSqlOperandTypeChecker(
      SqlOperandTypeChecker typeChecker, String functionName, boolean isUserDefinedFunction) {
    PPLTypeChecker pplTypeChecker;
    // Only the composite operand type checker for UDFs are concerned here.
    if (isUserDefinedFunction
        && typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
      // UDFs implement their own composite type checkers, which always use OR logic for
      // argument
      // types. Verifying the composition type would require accessing a protected field in
      // CompositeOperandTypeChecker. If access to this field is not allowed, type checking will
      // be skipped, so we avoid checking the composition type here.
      pplTypeChecker = PPLTypeChecker.wrapComposite(compositeTypeChecker, false);
    } else if (typeChecker instanceof ImplicitCastOperandTypeChecker implicitCastTypeChecker) {
      pplTypeChecker = PPLTypeChecker.wrapFamily(implicitCastTypeChecker);
    } else if (typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
      // If compositeTypeChecker contains operand checkers other than family type checkers or
      // other than OR compositions, the function with be registered with a null type checker,
      // which means the function will not be type checked.
      try {
        pplTypeChecker = PPLTypeChecker.wrapComposite(compositeTypeChecker, true);
      } catch (IllegalArgumentException | UnsupportedOperationException e) {
        logger.debug(
            String.format(
                "Failed to create composite type checker for operator: %s. Will skip its type"
                    + " checking",
                functionName),
            e);
        pplTypeChecker = null;
      }
    } else if (typeChecker instanceof SameOperandTypeChecker comparableTypeChecker) {
      // Comparison operators like EQUAL, GREATER_THAN, LESS_THAN, etc.
      // SameOperandTypeCheckers like COALESCE, IFNULL, etc.
      pplTypeChecker = PPLTypeChecker.wrapComparable(comparableTypeChecker);
    } else if (typeChecker instanceof UDFOperandMetadata.UDTOperandMetadata udtOperandMetadata) {
      pplTypeChecker = PPLTypeChecker.wrapUDT(udtOperandMetadata.allowedParamTypes());
    } else {
      logger.info(
          "Cannot create type checker for function: {}. Will skip its type checking", functionName);
      pplTypeChecker = null;
    }
    return pplTypeChecker;
  }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private abstract static class AbstractBuilder {

    /** Maps an operator to an implementation. */
    abstract void register(
        BuiltinFunctionName functionName, FunctionImp functionImp, PPLTypeChecker typeChecker);

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

        PPLTypeChecker pplTypeChecker =
            wrapSqlOperandTypeChecker(
                typeChecker, operator.getName(), operator instanceof SqlUserDefinedFunction);
        register(
            functionName,
            (RexBuilder builder, RexNode... args) -> builder.makeCall(operator, args),
            pplTypeChecker);
      }
    }

    private static SqlOperandTypeChecker extractTypeCheckerFromUDF(
        SqlUserDefinedFunction udfOperator) {
      UDFOperandMetadata udfOperandMetadata =
          (UDFOperandMetadata) udfOperator.getOperandTypeChecker();
      return (udfOperandMetadata == null) ? null : udfOperandMetadata.getInnerTypeChecker();
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
                      .toArray(RexNode[]::new))),
          null);
      register(
          JSON_OBJECT,
          ((builder, args) ->
              builder.makeCall(
                  SqlStdOperatorTable.JSON_OBJECT,
                  Stream.concat(Stream.of(builder.makeFlag(NULL_ON_NULL)), Arrays.stream(args))
                      .toArray(RexNode[]::new))),
          null);
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
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.BOTH),
                      builder.makeLiteral(" "),
                      arg),
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER));

      register(
          LTRIM,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.LEADING),
                      builder.makeLiteral(" "),
                      arg),
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER));
      register(
          RTRIM,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.TRAILING),
                      builder.makeLiteral(" "),
                      arg),
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER));
      register(
          ATAN,
          (FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlStdOperatorTable.ATAN2, arg1, arg2),
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
      register(
          STRCMP,
          (FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.STRCMP, arg2, arg1),
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
      // SqlStdOperatorTable.SUBSTRING.getOperandTypeChecker is null. We manually create a type
      // checker for it.
      register(
          SUBSTRING,
          (RexBuilder builder, RexNode... args) ->
              builder.makeCall(SqlStdOperatorTable.SUBSTRING, args),
          PPLTypeChecker.wrapComposite(
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
          (RexBuilder builder, RexNode... args) ->
              builder.makeCall(SqlStdOperatorTable.SUBSTRING, args),
          PPLTypeChecker.wrapComposite(
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
          (RexBuilder builder, RexNode... args) -> builder.makeCall(SqlStdOperatorTable.ITEM, args),
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER)
                      .or(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.ANY)),
              false));
      register(
          LOG,
          (FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.LOG, arg2, arg1),
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
      register(
          LOG,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlLibraryOperators.LOG,
                      arg,
                      builder.makeApproxLiteral(BigDecimal.valueOf(Math.E))),
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC));
      // SqlStdOperatorTable.SQRT is declared but not implemented. The call to SQRT in Calcite is
      // converted to POWER(x, 0.5).
      register(
          SQRT,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.POWER,
                      arg,
                      builder.makeApproxLiteral(BigDecimal.valueOf(0.5))),
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC));
      register(
          TYPEOF,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeLiteral(getLegacyTypeName(arg.getType(), QueryType.PPL)),
          null);
      register(
          XOR,
          (FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, arg1, arg2),
          PPLTypeChecker.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN));
      // SqlStdOperatorTable.CASE.getOperandTypeChecker is null. We manually create a type checker
      // for it. The second and third operands are required to be of the same type. If not,
      // it will throw an IllegalArgumentException with information Can't find leastRestrictive type
      register(
          IF,
          (RexBuilder builder, RexNode... args) -> builder.makeCall(SqlStdOperatorTable.CASE, args),
          PPLTypeChecker.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY));
      register(
          NULLIF,
          (FunctionImp2)
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlStdOperatorTable.CASE,
                      builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, arg2),
                      builder.makeNullLiteral(arg1.getType()),
                      arg1),
          PPLTypeChecker.wrapComparable((SameOperandTypeChecker) OperandTypes.SAME_SAME));
      register(
          IS_EMPTY,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.OR,
                      builder.makeCall(SqlStdOperatorTable.IS_NULL, arg),
                      builder.makeCall(SqlStdOperatorTable.IS_EMPTY, arg)),
          PPLTypeChecker.family(SqlTypeFamily.ANY));
      register(
          IS_BLANK,
          (FunctionImp1)
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
          PPLTypeChecker.family(SqlTypeFamily.ANY));
      register(
          LIKE,
          (FunctionImp2)
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlLibraryOperators.ILIKE,
                      arg1,
                      arg2,
                      // TODO: Figure out escaping solution. '\\' is used for JSON input but is not
                      // necessary for SQL function input
                      builder.makeLiteral("\\")),
          PPLTypeChecker.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING));
    }
  }

  private static class Builder extends AbstractBuilder {
    private final Map<BuiltinFunctionName, List<Pair<CalciteFuncSignature, FunctionImp>>> map =
        new HashMap<>();

    @Override
    void register(
        BuiltinFunctionName functionName, FunctionImp implement, PPLTypeChecker typeChecker) {
      CalciteFuncSignature signature =
          new CalciteFuncSignature(functionName.getName(), typeChecker);
      if (map.containsKey(functionName)) {
        map.get(functionName).add(Pair.of(signature, implement));
      } else {
        map.put(functionName, new ArrayList<>(List.of(Pair.of(signature, implement))));
      }
    }
  }

  private static class AggBuilder {
    private final Map<BuiltinFunctionName, Pair<CalciteFuncSignature, AggHandler>> map =
        new HashMap<>();

    void register(
        BuiltinFunctionName functionName, AggHandler aggHandler, PPLTypeChecker typeChecker) {
      CalciteFuncSignature signature =
          new CalciteFuncSignature(functionName.getName(), typeChecker);
      map.put(functionName, Pair.of(signature, aggHandler));
    }

    void registerOperator(BuiltinFunctionName functionName, SqlAggFunction aggFunction) {
      PPLTypeChecker typeChecker =
          wrapSqlOperandTypeChecker(aggFunction.getOperandTypeChecker(), functionName.name(), true);
      AggHandler handler =
          (distinct, field, argList, ctx) ->
              UserDefinedFunctionUtils.makeAggregateCall(
                  aggFunction, List.of(field), argList, ctx.relBuilder);
      register(functionName, handler, typeChecker);
    }

    void populate() {
      registerOperator(MAX, SqlStdOperatorTable.MAX);
      registerOperator(MIN, SqlStdOperatorTable.MIN);
      registerOperator(SUM, SqlStdOperatorTable.SUM);

      register(
          AVG,
          (distinct, field, argList, ctx) -> ctx.relBuilder.avg(distinct, null, field),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.AVG.getOperandTypeChecker(), AVG.name(), false));

      register(
          COUNT,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.count(
                  distinct, null, field == null ? ImmutableList.of() : ImmutableList.of(field)),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.COUNT.getOperandTypeChecker(), COUNT.name(), false));

      register(
          VARSAMP,
          (distinct, field, argList, ctx) -> ctx.relBuilder.aggregateCall(VAR_SAMP_NULLABLE, field),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.VAR_SAMP.getOperandTypeChecker(), VARSAMP.name(), false));

      register(
          VARPOP,
          (distinct, field, argList, ctx) -> ctx.relBuilder.aggregateCall(VAR_POP_NULLABLE, field),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.VAR_POP.getOperandTypeChecker(), VARPOP.name(), false));

      register(
          STDDEV_SAMP,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.aggregateCall(STDDEV_SAMP_NULLABLE, field),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.STDDEV_SAMP.getOperandTypeChecker(), STDDEV_SAMP.name(), false));

      register(
          STDDEV_POP,
          (distinct, field, argList, ctx) ->
              ctx.relBuilder.aggregateCall(STDDEV_POP_NULLABLE, field),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.STDDEV_POP.getOperandTypeChecker(), STDDEV_POP.name(), false));

      register(
          TAKE,
          (distinct, field, argList, ctx) -> {
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            return createAggregateFunction(
                TakeAggFunction.class,
                "TAKE",
                UserDefinedFunctionUtils.getReturnTypeInferenceForArray(),
                List.of(field),
                newArgList,
                ctx.relBuilder);
          },
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.ANY.or(
                      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER)),
              false));

      register(
          PERCENTILE_APPROX,
          (distinct, field, argList, ctx) -> {
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            newArgList.add(ctx.rexBuilder.makeFlag(field.getType().getSqlTypeName()));
            return createAggregateFunction(
                PercentileApproxFunction.class,
                "percentile_approx",
                ReturnTypes.ARG0_FORCE_NULLABLE,
                List.of(field),
                newArgList,
                ctx.relBuilder);
          },
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.NUMERIC_NUMERIC.or(
                      OperandTypes.family(
                          SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
              false));

      register(
          INTERNAL_PATTERN,
          (distinct, field, argList, ctx) ->
              createAggregateFunction(
                  LogPatternAggFunction.class,
                  "pattern",
                  ReturnTypes.explicit(UserDefinedFunctionUtils.nullablePatternAggList),
                  List.of(field),
                  argList,
                  ctx.relBuilder),
          null);

      register(
          LIST,
          (distinct, field, argList, ctx) ->
              createAggregateFunction(
                  ListAggFunction.class,
                  "list",
                  UserDefinedFunctionUtils.getReturnTypeInferenceForArray(),
                  List.of(field),
                  argList,
                  ctx.relBuilder),
          PPLTypeChecker.family(SqlTypeFamily.ANY));

      register(
          VALUES,
          (distinct, field, argList, ctx) ->
              createAggregateFunction(
                  ValuesAggFunction.class,
                  "values",
                  UserDefinedFunctionUtils.getReturnTypeInferenceForArray(),
                  List.of(field),
                  argList,
                  ctx.relBuilder),
          PPLTypeChecker.family(SqlTypeFamily.ANY));
    }
  }
}
