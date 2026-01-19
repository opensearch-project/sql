/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ABS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ACOS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.AND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_SLICE;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.FIRST;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ILIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_APPEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_GROK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_ITEM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_PARSE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_PATTERN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_PATTERN_PARSER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_5;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_PG_4;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_TRANSLATE3;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTRACT_ALL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_KEYS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_OBJECT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_SET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_VALID;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LAST;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAP_APPEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAP_CONCAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAP_REMOVE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH_BOOL_PREFIX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH_PHRASE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MATCH_PHRASE_PREFIX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MD5;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MEDIAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MICROSECOND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MINSPAN_BUCKET;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVAPPEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVDEDUP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVFIND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVINDEX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVJOIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVMAP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MVZIP;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RANGE_BUCKET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REDUCE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REGEXP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REGEXP_MATCH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REPLACE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REVERSE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REX_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REX_EXTRACT_MULTI;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REX_OFFSET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RIGHT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RINT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ROUND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RTRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SCALAR_MAX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SCALAR_MIN;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SPAN_BUCKET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SPLIT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SQRT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STDDEV_POP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STDDEV_SAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STRCMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STRFTIME;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TONUMBER;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TOSTRING;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.VALUES;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.VARPOP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.VARSAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEKDAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEKOFYEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WIDTH_BUCKET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.XOR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.YEARWEEK;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.CollectionUDF.MVIndexFunctionImp;

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

  public interface FunctionImp3 extends FunctionImp {
    RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2, RexNode arg3);

    @Override
    default RexNode resolve(RexBuilder builder, RexNode... args) {
      if (args.length != 3) {
        throw new IllegalArgumentException("This function requires exactly 3 arguments");
      }
      return resolve(builder, args[0], args[1], args[2]);
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
  private final ImmutableMap<BuiltinFunctionName, FunctionImp> functionRegistry;

  /**
   * The external function registry. Functions whose implementations depend on a specific data
   * engine should be registered here. This reduces coupling between the core module and particular
   * storage backends.
   */
  private final Map<BuiltinFunctionName, FunctionImp> externalFunctionRegistry;

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
    final ImmutableMap.Builder<BuiltinFunctionName, FunctionImp> mapBuilder =
        ImmutableMap.builder();
    mapBuilder.putAll(builder.map);
    this.functionRegistry = ImmutableMap.copyOf(mapBuilder.build());
    this.externalFunctionRegistry = new ConcurrentHashMap<>();

    final ImmutableMap.Builder<BuiltinFunctionName, AggHandler> aggMapBuilder =
        ImmutableMap.builder();
    aggMapBuilder.putAll(aggBuilder.map);
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
    if (externalFunctionRegistry.containsKey(functionName)) {
      logger.warn(
          String.format(Locale.ROOT, "Function %s is registered multiple times", functionName));
    }
    externalFunctionRegistry.put(functionName, (builder, args) -> builder.makeCall(operator, args));
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
    if (aggExternalFunctionRegistry.containsKey(functionName)) {
      logger.warn(
          String.format(
              Locale.ROOT, "Aggregate function %s is registered multiple times", functionName));
    }
    AggHandler handler =
        (distinct, field, argList, ctx) ->
            UserDefinedFunctionUtils.makeAggregateCall(
                aggFunction, List.of(field), argList, ctx.relBuilder);
    aggExternalFunctionRegistry.put(functionName, handler);
  }

  public RelBuilder.AggCall resolveAgg(
      BuiltinFunctionName functionName,
      boolean distinct,
      RexNode field,
      List<RexNode> argList,
      CalcitePlanContext context) {
    var implementation = getImplementation(functionName);
    return implementation.apply(distinct, field, argList, context);
  }

  private AggHandler getImplementation(BuiltinFunctionName functionName) {
    var implementation = aggExternalFunctionRegistry.get(functionName);
    if (implementation == null) {
      implementation = aggFunctionRegistry.get(functionName);
    }
    if (implementation == null) {
      throw new IllegalStateException(String.format("Cannot resolve function: %s", functionName));
    }
    return implementation;
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
    //  If the function is not part of the external registry, check the internal registry.
    FunctionImp implementation =
        externalFunctionRegistry.get(functionName) != null
            ? externalFunctionRegistry.get(functionName)
            : functionRegistry.get(functionName);
    if (implementation == null) {
      throw new IllegalStateException(String.format("Cannot resolve function: %s", functionName));
    }

    // Make compulsory casts for some functions that require specific casting of arguments.
    // For example, the REDUCE function requires the second argument to be cast to the
    // return type of the lambda function.
    compulsoryCast(builder, functionName, args);
    return implementation.resolve(builder, args);
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

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private abstract static class AbstractBuilder {

    /** Maps an operator to an implementation. */
    abstract void register(BuiltinFunctionName functionName, FunctionImp functionImp);

    /**
     * Registers an operator for a built-in function name.
     *
     * <p>Each function name can only be registered to one operator. Use {@code
     * register(BuiltinFunctionName, FunctionImp)} to dynamically register a built-in function name
     * to different operators based on argument count or types if override is desired.
     *
     * @param functionName the built-in function name under which to register the operators
     * @param operator the operator to associate with this function name
     */
    protected void registerOperator(BuiltinFunctionName functionName, SqlOperator operator) {
      register(
          functionName, (RexBuilder builder, RexNode... args) -> builder.makeCall(operator, args));
    }

    protected void registerDivideFunction(BuiltinFunctionName functionName) {
      register(
          functionName,
          (FunctionImp2)
              (builder, left, right) -> {
                SqlOperator operator =
                    CalcitePlanContext.isLegacyPreferred()
                        ? PPLBuiltinOperators.DIVIDE
                        : SqlLibraryOperators.SAFE_DIVIDE;
                return builder.makeCall(operator, left, right);
              });
    }

    void populate() {
      // register operators for comparison
      registerOperator(NOTEQUAL, SqlStdOperatorTable.NOT_EQUALS);
      registerOperator(EQUAL, SqlStdOperatorTable.EQUALS);
      registerOperator(GREATER, SqlStdOperatorTable.GREATER_THAN);
      registerOperator(GTE, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
      registerOperator(LESS, SqlStdOperatorTable.LESS_THAN);
      registerOperator(LTE, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

      // Register std operator
      registerOperator(AND, SqlStdOperatorTable.AND);
      registerOperator(OR, SqlStdOperatorTable.OR);
      registerOperator(NOT, SqlStdOperatorTable.NOT);
      registerOperator(SUBTRACTFUNCTION, SqlStdOperatorTable.MINUS);
      registerOperator(SUBTRACT, SqlStdOperatorTable.MINUS);
      registerOperator(MULTIPLY, SqlStdOperatorTable.MULTIPLY);
      registerOperator(MULTIPLYFUNCTION, SqlStdOperatorTable.MULTIPLY);
      registerOperator(TRUNCATE, SqlStdOperatorTable.TRUNCATE);
      registerOperator(ASCII, SqlStdOperatorTable.ASCII);
      registerOperator(LENGTH, SqlStdOperatorTable.CHAR_LENGTH);
      registerOperator(LOWER, SqlStdOperatorTable.LOWER);
      registerOperator(POSITION, SqlStdOperatorTable.POSITION);
      registerOperator(LOCATE, SqlStdOperatorTable.POSITION);
      // Register REPLACE with automatic PCRE-to-Java backreference conversion
      register(
          REPLACE,
          (RexBuilder builder, RexNode... args) -> {
            // Validate regex pattern at query planning time
            if (args.length >= 2 && args[1] instanceof RexLiteral) {
              RexLiteral patternLiteral = (RexLiteral) args[1];
              String pattern = patternLiteral.getValueAs(String.class);
              if (pattern != null) {
                try {
                  // Compile pattern to validate it - this will throw PatternSyntaxException if
                  // invalid
                  Pattern.compile(pattern);
                } catch (PatternSyntaxException e) {
                  // Convert to IllegalArgumentException so it's treated as a client error (400)
                  throw new IllegalArgumentException(
                      String.format("Invalid regex pattern '%s': %s", pattern, e.getDescription()),
                      e);
                }
              }
            }

            if (args.length == 3 && args[2] instanceof RexLiteral) {
              RexLiteral literal = (RexLiteral) args[2];
              String replacement = literal.getValueAs(String.class);
              if (replacement != null) {
                // Convert PCRE/sed backreferences (\1, \2) to Java style ($1, $2)
                String javaReplacement = replacement.replaceAll("\\\\(\\d+)", "\\$$1");
                if (!javaReplacement.equals(replacement)) {
                  RexNode convertedLiteral =
                      builder.makeLiteral(
                          javaReplacement,
                          literal.getType(),
                          literal.getTypeName() != SqlTypeName.CHAR);
                  return builder.makeCall(
                      SqlLibraryOperators.REGEXP_REPLACE_3, args[0], args[1], convertedLiteral);
                }
              }
            }
            return builder.makeCall(SqlLibraryOperators.REGEXP_REPLACE_3, args);
          });
      registerOperator(UPPER, SqlStdOperatorTable.UPPER);
      registerOperator(ABS, SqlStdOperatorTable.ABS);
      registerOperator(ACOS, SqlStdOperatorTable.ACOS);
      registerOperator(ASIN, SqlStdOperatorTable.ASIN);
      registerOperator(ATAN, PPLBuiltinOperators.ATAN);
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

      registerOperator(IFNULL, SqlStdOperatorTable.COALESCE);
      registerOperator(EARLIEST, PPLBuiltinOperators.EARLIEST);
      registerOperator(LATEST, PPLBuiltinOperators.LATEST);
      registerOperator(COALESCE, SqlStdOperatorTable.COALESCE);

      // Register library operator
      registerOperator(REGEXP, PPLBuiltinOperators.REGEXP);
      registerOperator(REGEXP_MATCH, SqlLibraryOperators.REGEXP_CONTAINS);
      registerOperator(CONCAT, SqlLibraryOperators.CONCAT_FUNCTION);
      registerOperator(CONCAT_WS, SqlLibraryOperators.CONCAT_WS);
      registerOperator(REVERSE, SqlLibraryOperators.REVERSE);
      registerOperator(RIGHT, SqlLibraryOperators.RIGHT);
      registerOperator(LEFT, SqlLibraryOperators.LEFT);
      registerOperator(LOG, SqlLibraryOperators.LOG_MYSQL);
      registerOperator(LOG2, SqlLibraryOperators.LOG2);
      registerOperator(MD5, SqlLibraryOperators.MD5);
      registerOperator(SHA1, SqlLibraryOperators.SHA1);
      registerOperator(CRC32, SqlLibraryOperators.CRC32);
      registerOperator(INTERNAL_REGEXP_REPLACE_PG_4, SqlLibraryOperators.REGEXP_REPLACE_PG_4);
      registerOperator(INTERNAL_REGEXP_REPLACE_5, SqlLibraryOperators.REGEXP_REPLACE_5);
      registerOperator(INTERNAL_TRANSLATE3, SqlLibraryOperators.TRANSLATE3);

      // Register eval functions for PPL scalar max() and min() calls
      registerOperator(SCALAR_MAX, PPLBuiltinOperators.SCALAR_MAX);
      registerOperator(SCALAR_MIN, PPLBuiltinOperators.SCALAR_MIN);

      // Register PPL UDF operator
      registerOperator(COSH, PPLBuiltinOperators.COSH);
      registerOperator(SINH, PPLBuiltinOperators.SINH);
      registerOperator(EXPM1, PPLBuiltinOperators.EXPM1);
      registerOperator(RINT, PPLBuiltinOperators.RINT);
      registerOperator(SPAN, PPLBuiltinOperators.SPAN);
      registerOperator(SPAN_BUCKET, PPLBuiltinOperators.SPAN_BUCKET);
      registerOperator(WIDTH_BUCKET, PPLBuiltinOperators.WIDTH_BUCKET);
      registerOperator(MINSPAN_BUCKET, PPLBuiltinOperators.MINSPAN_BUCKET);
      registerOperator(RANGE_BUCKET, PPLBuiltinOperators.RANGE_BUCKET);
      registerOperator(E, PPLBuiltinOperators.E);
      registerOperator(CONV, PPLBuiltinOperators.CONV);
      registerOperator(MOD, PPLBuiltinOperators.MOD);
      registerOperator(MODULUS, PPLBuiltinOperators.MOD);
      registerOperator(MODULUSFUNCTION, PPLBuiltinOperators.MOD);
      registerDivideFunction(DIVIDE);
      registerDivideFunction(DIVIDEFUNCTION);
      registerOperator(SHA2, PPLBuiltinOperators.SHA2);
      registerOperator(CIDRMATCH, PPLBuiltinOperators.CIDRMATCH);
      registerOperator(INTERNAL_GROK, PPLBuiltinOperators.GROK);
      registerOperator(INTERNAL_PARSE, PPLBuiltinOperators.PARSE);
      registerOperator(MATCH, PPLBuiltinOperators.MATCH);
      registerOperator(MATCH_PHRASE, PPLBuiltinOperators.MATCH_PHRASE);
      registerOperator(MATCH_BOOL_PREFIX, PPLBuiltinOperators.MATCH_BOOL_PREFIX);
      registerOperator(MATCH_PHRASE_PREFIX, PPLBuiltinOperators.MATCH_PHRASE_PREFIX);
      registerOperator(SIMPLE_QUERY_STRING, PPLBuiltinOperators.SIMPLE_QUERY_STRING);
      registerOperator(QUERY_STRING, PPLBuiltinOperators.QUERY_STRING);
      registerOperator(MULTI_MATCH, PPLBuiltinOperators.MULTI_MATCH);
      registerOperator(REX_EXTRACT, PPLBuiltinOperators.REX_EXTRACT);
      registerOperator(REX_EXTRACT_MULTI, PPLBuiltinOperators.REX_EXTRACT_MULTI);
      registerOperator(REX_OFFSET, PPLBuiltinOperators.REX_OFFSET);

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
      registerOperator(STRFTIME, PPLBuiltinOperators.STRFTIME);
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
      registerOperator(TONUMBER, PPLBuiltinOperators.TONUMBER);
      register(
          TOSTRING,
          (builder, args) -> {
            if (args.length == 1) {
              return builder.makeCast(
                  TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true), args[0]);
            }
            return builder.makeCall(PPLBuiltinOperators.TOSTRING, args);
          });

      // Register MVJOIN to use Calcite's ARRAY_JOIN
      register(
          MVJOIN,
          (FunctionImp2)
              (builder, array, delimiter) ->
                  builder.makeCall(SqlLibraryOperators.ARRAY_JOIN, array, delimiter));

      // Register SPLIT with custom logic for empty delimiter
      // Case 1: Delimiter is not empty string, use SPLIT
      // Case 2: Delimiter is empty string, use REGEXP_EXTRACT_ALL with '.' pattern
      register(
          SPLIT,
          (FunctionImp2)
              (builder, str, delimiter) -> {
                // Create condition: delimiter = ''
                RexNode emptyString = builder.makeLiteral("");
                RexNode isEmptyDelimiter =
                    builder.makeCall(SqlStdOperatorTable.EQUALS, delimiter, emptyString);

                // For empty delimiter: split into characters using REGEXP_EXTRACT_ALL with '.'
                // pattern This matches each individual character
                RexNode dotPattern = builder.makeLiteral(".");
                RexNode splitChars =
                    builder.makeCall(SqlLibraryOperators.REGEXP_EXTRACT_ALL, str, dotPattern);

                // For non-empty delimiter: use standard SPLIT
                RexNode normalSplit = builder.makeCall(SqlLibraryOperators.SPLIT, str, delimiter);

                // Use CASE to choose between the two approaches
                // CASE WHEN isEmptyDelimiter THEN splitChars ELSE normalSplit END
                return builder.makeCall(
                    SqlStdOperatorTable.CASE, isEmptyDelimiter, splitChars, normalSplit);
              });

      // Register MVINDEX to use Calcite's ITEM/ARRAY_SLICE with index normalization
      register(MVINDEX, new MVIndexFunctionImp());

      registerOperator(ARRAY, PPLBuiltinOperators.ARRAY);
      registerOperator(MVAPPEND, PPLBuiltinOperators.MVAPPEND);
      registerOperator(INTERNAL_APPEND, PPLBuiltinOperators.INTERNAL_APPEND);
      registerOperator(MVDEDUP, SqlLibraryOperators.ARRAY_DISTINCT);
      registerOperator(MVFIND, PPLBuiltinOperators.MVFIND);
      registerOperator(MVZIP, PPLBuiltinOperators.MVZIP);
      registerOperator(MVMAP, PPLBuiltinOperators.TRANSFORM);
      registerOperator(MAP_APPEND, PPLBuiltinOperators.MAP_APPEND);
      registerOperator(MAP_CONCAT, SqlLibraryOperators.MAP_CONCAT);
      registerOperator(MAP_REMOVE, PPLBuiltinOperators.MAP_REMOVE);
      registerOperator(ARRAY_LENGTH, SqlLibraryOperators.ARRAY_LENGTH);
      registerOperator(ARRAY_SLICE, SqlLibraryOperators.ARRAY_SLICE);
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
      registerOperator(JSON_EXTRACT_ALL, PPLBuiltinOperators.JSON_EXTRACT_ALL); // internal

      // Register ADD (+ symbol) for string concatenation and numeric addition
      // Not creating PPL builtin operator as it will cause confusion during function resolution
      FunctionImp add =
          (builder, args) -> {
            SqlOperator op =
                (Stream.of(args).map(RexNode::getType).allMatch(OpenSearchTypeUtil::isCharacter))
                    ? SqlStdOperatorTable.CONCAT
                    : SqlStdOperatorTable.PLUS;
            return builder.makeCall(op, args);
          };
      register(ADD, add);
      register(ADDFUNCTION, add);
      registerOperator(INTERNAL_ITEM, SqlStdOperatorTable.ITEM);
      registerOperator(XOR, SqlStdOperatorTable.NOT_EQUALS);
      registerOperator(IF, SqlStdOperatorTable.CASE);
      registerOperator(IS_NOT_NULL, SqlStdOperatorTable.IS_NOT_NULL);
      registerOperator(IS_PRESENT, SqlStdOperatorTable.IS_NOT_NULL);
      registerOperator(IS_NULL, SqlStdOperatorTable.IS_NULL);

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
                      arg));

      register(
          LTRIM,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.LEADING),
                      builder.makeLiteral(" "),
                      arg));
      register(
          RTRIM,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.TRIM,
                      builder.makeFlag(Flag.TRAILING),
                      builder.makeLiteral(" "),
                      arg));
      register(
          STRCMP,
          (FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.STRCMP, arg2, arg1));
      register(
          SUBSTRING,
          (RexBuilder builder, RexNode... args) ->
              builder.makeCall(SqlStdOperatorTable.SUBSTRING, args));
      register(
          SUBSTR,
          (RexBuilder builder, RexNode... args) ->
              builder.makeCall(SqlStdOperatorTable.SUBSTRING, args));
      // SqlStdOperatorTable.SQRT is declared but not implemented. The call to SQRT in Calcite is
      // converted to POWER(x, 0.5).
      register(
          SQRT,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.POWER,
                      arg,
                      builder.makeApproxLiteral(BigDecimal.valueOf(0.5))));
      register(
          TYPEOF,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeLiteral(getLegacyTypeName(arg.getType(), QueryType.PPL)));
      register(
          NULLIF,
          (FunctionImp2)
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlStdOperatorTable.CASE,
                      builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, arg2),
                      builder.makeNullLiteral(arg1.getType()),
                      arg1));
      register(
          IS_EMPTY,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.OR,
                      builder.makeCall(SqlStdOperatorTable.IS_NULL, arg),
                      builder.makeCall(SqlStdOperatorTable.EQUALS, arg, builder.makeLiteral(""))));
      register(
          IS_BLANK,
          (FunctionImp1)
              (builder, arg) ->
                  builder.makeCall(
                      SqlStdOperatorTable.OR,
                      builder.makeCall(SqlStdOperatorTable.IS_NULL, arg),
                      builder.makeCall(
                          SqlStdOperatorTable.EQUALS,
                          builder.makeCall(
                              SqlStdOperatorTable.TRIM,
                              builder.makeFlag(Flag.BOTH),
                              builder.makeLiteral(" "),
                              arg),
                          builder.makeLiteral(""))));
      register(
          ILIKE,
          (FunctionImp2)
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlLibraryOperators.ILIKE, arg1, arg2, builder.makeLiteral("\\")));
      register(
          LIKE,
          (FunctionImp3)
              (builder, arg1, arg2, arg3) ->
                  ((RexLiteral) arg3).getValueAs(Boolean.class)
                      ? builder.makeCall(
                          SqlStdOperatorTable.LIKE, arg1, arg2, builder.makeLiteral("\\"))
                      : builder.makeCall(
                          SqlLibraryOperators.ILIKE, arg1, arg2, builder.makeLiteral("\\")));
    }
  }

  private static class Builder extends AbstractBuilder {
    private final Map<BuiltinFunctionName, FunctionImp> map = new HashMap<>();

    @Override
    void register(BuiltinFunctionName functionName, FunctionImp implement) {
      if (map.containsKey(functionName)) {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Each function can only be registered with one operator: %s",
                functionName));
      }
      map.put(functionName, implement);
    }
  }

  private static class AggBuilder {
    private static final double MEDIAN_PERCENTILE = 50.0;
    private final Map<BuiltinFunctionName, AggHandler> map = new HashMap<>();

    void register(BuiltinFunctionName functionName, AggHandler aggHandler) {
      map.put(functionName, aggHandler);
    }

    void registerOperator(BuiltinFunctionName functionName, SqlAggFunction aggFunction) {
      AggHandler handler =
          (distinct, field, argList, ctx) -> {
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            return UserDefinedFunctionUtils.makeAggregateCall(
                aggFunction, List.of(field), newArgList, ctx.relBuilder);
          };
      register(functionName, handler);
    }

    void populate() {
      registerOperator(MAX, SqlStdOperatorTable.MAX);
      registerOperator(MIN, SqlStdOperatorTable.MIN);
      registerOperator(SUM, SqlStdOperatorTable.SUM);
      registerOperator(VARSAMP, PPLBuiltinOperators.VAR_SAMP_NULLABLE);
      registerOperator(VARPOP, PPLBuiltinOperators.VAR_POP_NULLABLE);
      registerOperator(STDDEV_SAMP, PPLBuiltinOperators.STDDEV_SAMP_NULLABLE);
      registerOperator(STDDEV_POP, PPLBuiltinOperators.STDDEV_POP_NULLABLE);
      registerOperator(TAKE, PPLBuiltinOperators.TAKE);
      registerOperator(INTERNAL_PATTERN, PPLBuiltinOperators.INTERNAL_PATTERN);
      registerOperator(LIST, PPLBuiltinOperators.LIST);
      registerOperator(VALUES, PPLBuiltinOperators.VALUES);

      register(AVG, (distinct, field, argList, ctx) -> ctx.relBuilder.avg(distinct, null, field));

      register(
          COUNT,
          (distinct, field, argList, ctx) -> {
            if (field == null) {
              // count() without arguments should count all rows
              return ctx.relBuilder.count(distinct, null);
            } else {
              // count(field) should count non-null values of the field
              return ctx.relBuilder.count(distinct, null, field);
            }
          });

      register(
          PERCENTILE_APPROX,
          (distinct, field, argList, ctx) -> {
            if (field.getType() == null) {
              throw new IllegalArgumentException("Field type cannot be null");
            }
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            newArgList.add(ctx.rexBuilder.makeFlag(field.getType().getSqlTypeName()));
            return UserDefinedFunctionUtils.makeAggregateCall(
                PPLBuiltinOperators.PERCENTILE_APPROX, List.of(field), newArgList, ctx.relBuilder);
          });

      register(
          MEDIAN,
          (distinct, field, argList, ctx) -> {
            if (distinct) {
              throw new IllegalArgumentException("MEDIAN does not support DISTINCT");
            }
            if (!argList.isEmpty()) {
              throw new IllegalArgumentException("MEDIAN takes no additional arguments");
            }
            if (field.getType() == null) {
              throw new IllegalArgumentException("Field type cannot be null");
            }
            List<RexNode> medianArgList =
                List.of(
                    ctx.rexBuilder.makeExactLiteral(BigDecimal.valueOf(MEDIAN_PERCENTILE)),
                    ctx.rexBuilder.makeFlag(field.getType().getSqlTypeName()));
            return UserDefinedFunctionUtils.makeAggregateCall(
                PPLBuiltinOperators.PERCENTILE_APPROX,
                List.of(field),
                medianArgList,
                ctx.relBuilder);
          });

      register(
          EARLIEST,
          (distinct, field, argList, ctx) -> {
            List<RexNode> args = resolveTimeField(argList, ctx);
            return UserDefinedFunctionUtils.makeAggregateCall(
                SqlStdOperatorTable.ARG_MIN, List.of(field), args, ctx.relBuilder);
          });

      register(
          LATEST,
          (distinct, field, argList, ctx) -> {
            List<RexNode> args = resolveTimeField(argList, ctx);
            return UserDefinedFunctionUtils.makeAggregateCall(
                SqlStdOperatorTable.ARG_MAX, List.of(field), args, ctx.relBuilder);
          });

      // Register FIRST function - uses document order
      register(
          FIRST,
          (distinct, field, argList, ctx) -> {
            // Use our custom FirstAggFunction for document order aggregation
            return ctx.relBuilder.aggregateCall(PPLBuiltinOperators.FIRST, field);
          });

      // Register LAST function - uses document order
      register(
          LAST,
          (distinct, field, argList, ctx) -> {
            // Use our custom LastAggFunction for document order aggregation
            return ctx.relBuilder.aggregateCall(PPLBuiltinOperators.LAST, field);
          });
    }
  }

  static List<RexNode> resolveTimeField(List<RexNode> argList, CalcitePlanContext ctx) {
    if (argList.isEmpty()) {
      // Try to find @timestamp field
      var timestampField = ctx.relBuilder.peek().getRowType().getField("@timestamp", false, false);
      if (timestampField == null) {
        throw new IllegalArgumentException(
            "Default @timestamp field not found. Please specify a time field explicitly.");
      }
      return List.of(
          ctx.rexBuilder.makeInputRef(timestampField.getType(), timestampField.getIndex()));
    } else {
      return argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
    }
  }
}
