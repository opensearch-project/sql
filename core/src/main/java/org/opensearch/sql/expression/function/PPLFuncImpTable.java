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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.AUTO;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NONE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOTEQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOW;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NULLIF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NUM;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RMCOMMA;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RMUNIT;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.ImplicitCastOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
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
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
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

  public List<RexNode> validateAggFunctionSignature(
      BuiltinFunctionName functionName,
      RexNode field,
      List<RexNode> argList,
      RexBuilder rexBuilder) {
    var implementation = getImplementation(functionName);
    return validateFunctionArgs(implementation, functionName, field, argList, rexBuilder);
  }

  public RelBuilder.AggCall resolveAgg(
      BuiltinFunctionName functionName,
      boolean distinct,
      RexNode field,
      List<RexNode> argList,
      CalcitePlanContext context) {
    var implementation = getImplementation(functionName);

    // Validation is done based on original argument types to generate error from user perspective.
    List<RexNode> nodes =
        validateFunctionArgs(implementation, functionName, field, argList, context.rexBuilder);

    var handler = implementation.getValue();
    return nodes != null
        ? handler.apply(distinct, nodes.getFirst(), nodes.subList(1, nodes.size()), context)
        : handler.apply(distinct, field, argList, context);
  }

  static List<RexNode> validateFunctionArgs(
      Pair<CalciteFuncSignature, AggHandler> implementation,
      BuiltinFunctionName functionName,
      RexNode field,
      List<RexNode> argList,
      RexBuilder rexBuilder) {
    CalciteFuncSignature signature = implementation.getKey();

    List<RelDataType> argTypes = new ArrayList<>();
    if (field != null) {
      argTypes.add(field.getType());
    }

    // Currently only PERCENTILE_APPROX, TAKE, EARLIEST, and LATEST have additional arguments.
    // Their additional arguments will always come as a map of <argName, value>
    List<RelDataType> additionalArgTypes =
        argList.stream().map(PlanUtils::derefMapCall).map(RexNode::getType).toList();
    argTypes.addAll(additionalArgTypes);
    List<RexNode> coercionNodes = null;
    if (!signature.match(functionName.getName(), argTypes)) {
      List<RexNode> fields = new ArrayList<>();
      fields.add(field);
      fields.addAll(argList);
      if (CoercionUtils.hasString(fields)) {
        coercionNodes = CoercionUtils.castArguments(rexBuilder, signature.typeChecker(), fields);
      }
      if (coercionNodes == null) {
        String errorMessagePattern =
            argTypes.size() <= 1
                ? "Aggregation function %s expects field type {%s}, but got %s"
                : "Aggregation function %s expects field type and additional arguments {%s}, but"
                    + " got %s";
        throw new ExpressionEvaluationException(
            String.format(
                errorMessagePattern,
                functionName,
                signature.typeChecker().getAllowedSignatures(),
                PlanUtils.getActualSignature(argTypes)));
      }
    }
    return coercionNodes;
  }

  private Pair<CalciteFuncSignature, AggHandler> getImplementation(
      BuiltinFunctionName functionName) {
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
              functionName, PlanUtils.getActualSignature(argTypes), e.getMessage()),
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
            functionName, allowedSignatures, PlanUtils.getActualSignature(argTypes)));
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
    protected void registerOperator(BuiltinFunctionName functionName, SqlOperator... operators) {
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
        registerOperator(functionName, operator, pplTypeChecker);
      }
    }

    /**
     * Registers an operator for a built-in function name with a specified {@link PPLTypeChecker}.
     * This allows custom type checking logic to be associated with the operator.
     *
     * @param functionName the built-in function name
     * @param operator the SQL operator to register
     * @param typeChecker the type checker to use for validating argument types
     */
    protected void registerOperator(
        BuiltinFunctionName functionName, SqlOperator operator, PPLTypeChecker typeChecker) {
      register(
          functionName,
          (RexBuilder builder, RexNode... args) -> builder.makeCall(operator, args),
          typeChecker);
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
              },
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
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

      // Register ADDFUNCTION for numeric addition only
      registerOperator(ADDFUNCTION, SqlStdOperatorTable.PLUS);
      registerOperator(
          SUBTRACTFUNCTION,
          SqlStdOperatorTable.MINUS,
          PPLTypeChecker.wrapFamily((FamilyOperandTypeChecker) OperandTypes.NUMERIC_NUMERIC));
      registerOperator(
          SUBTRACT,
          SqlStdOperatorTable.MINUS,
          PPLTypeChecker.wrapFamily((FamilyOperandTypeChecker) OperandTypes.NUMERIC_NUMERIC));
      // Add DATETIME-DATETIME variant for timestamp binning support
      registerOperator(
          SUBTRACT,
          SqlStdOperatorTable.MINUS,
          PPLTypeChecker.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME));
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
          },
          wrapSqlOperandTypeChecker(
              SqlLibraryOperators.REGEXP_REPLACE_3.getOperandTypeChecker(), REPLACE.name(), false));
      registerOperator(UPPER, SqlStdOperatorTable.UPPER);
      registerOperator(ABS, SqlStdOperatorTable.ABS);
      registerOperator(ACOS, SqlStdOperatorTable.ACOS);
      registerOperator(ASIN, SqlStdOperatorTable.ASIN);
      registerOperator(ATAN, SqlStdOperatorTable.ATAN);
      registerOperator(ATAN2, SqlStdOperatorTable.ATAN2);
      // TODO, workaround to support sequence CompositeOperandTypeChecker.
      registerOperator(
          CEIL,
          SqlStdOperatorTable.CEIL,
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.NUMERIC_OR_INTERVAL.or(
                      OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.ANY)),
              false));
      // TODO, workaround to support sequence CompositeOperandTypeChecker.
      registerOperator(
          CEILING,
          SqlStdOperatorTable.CEIL,
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.NUMERIC_OR_INTERVAL.or(
                      OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.ANY)),
              false));
      registerOperator(COS, SqlStdOperatorTable.COS);
      registerOperator(COT, SqlStdOperatorTable.COT);
      registerOperator(DEGREES, SqlStdOperatorTable.DEGREES);
      registerOperator(EXP, SqlStdOperatorTable.EXP);
      // TODO, workaround to support sequence CompositeOperandTypeChecker.
      registerOperator(
          FLOOR,
          SqlStdOperatorTable.FLOOR,
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.NUMERIC_OR_INTERVAL.or(
                      OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.ANY)),
              false));
      registerOperator(LN, SqlStdOperatorTable.LN);
      registerOperator(LOG10, SqlStdOperatorTable.LOG10);
      registerOperator(PI, SqlStdOperatorTable.PI);
      registerOperator(POW, SqlStdOperatorTable.POWER);
      registerOperator(POWER, SqlStdOperatorTable.POWER);
      registerOperator(RADIANS, SqlStdOperatorTable.RADIANS);
      registerOperator(RAND, SqlStdOperatorTable.RAND);
      // TODO, workaround to support sequence CompositeOperandTypeChecker.
      registerOperator(
          ROUND,
          SqlStdOperatorTable.ROUND,
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.NUMERIC.or(
                      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER)),
              false));
      registerOperator(SIGN, SqlStdOperatorTable.SIGN);
      registerOperator(SIGNUM, SqlStdOperatorTable.SIGN);
      registerOperator(SIN, SqlStdOperatorTable.SIN);
      registerOperator(CBRT, SqlStdOperatorTable.CBRT);

      registerOperator(IFNULL, SqlStdOperatorTable.COALESCE);
      registerOperator(EARLIEST, PPLBuiltinOperators.EARLIEST);
      registerOperator(LATEST, PPLBuiltinOperators.LATEST);
      registerOperator(COALESCE, PPLBuiltinOperators.ENHANCED_COALESCE);

      // Register library operator
      registerOperator(REGEXP, SqlLibraryOperators.REGEXP);
      registerOperator(REGEXP_MATCH, SqlLibraryOperators.REGEXP_CONTAINS);
      registerOperator(CONCAT, SqlLibraryOperators.CONCAT_FUNCTION);
      registerOperator(CONCAT_WS, SqlLibraryOperators.CONCAT_WS);
      registerOperator(CONCAT_WS, SqlLibraryOperators.CONCAT_WS);
      registerOperator(REVERSE, SqlLibraryOperators.REVERSE);
      registerOperator(RIGHT, SqlLibraryOperators.RIGHT);
      registerOperator(LEFT, SqlLibraryOperators.LEFT);
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
      registerOperator(TOSTRING, PPLBuiltinOperators.TOSTRING);

      // Register PPL Convert command functions
      registerOperator(AUTO, PPLBuiltinOperators.AUTO);
      registerOperator(NUM, PPLBuiltinOperators.NUM);
      registerOperator(RMCOMMA, PPLBuiltinOperators.RMCOMMA);
      registerOperator(RMUNIT, PPLBuiltinOperators.RMUNIT);
      registerOperator(NONE, PPLBuiltinOperators.NONE);

      register(
          TOSTRING,
          (FunctionImp1)
              (builder, source) ->
                  builder.makeCast(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true), source),
          PPLTypeChecker.family(SqlTypeFamily.ANY));

      // Register MVJOIN to use Calcite's ARRAY_JOIN
      register(
          MVJOIN,
          (FunctionImp2)
              (builder, array, delimiter) ->
                  builder.makeCall(SqlLibraryOperators.ARRAY_JOIN, array, delimiter),
          PPLTypeChecker.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER));

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
              },
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));

      // Register MVINDEX to use Calcite's ITEM/ARRAY_SLICE with index normalization
      register(
          MVINDEX,
          new MVIndexFunctionImp(),
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER)
                      .or(
                          OperandTypes.family(
                              SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)),
              false));

      registerOperator(ARRAY, PPLBuiltinOperators.ARRAY);
      registerOperator(MVAPPEND, PPLBuiltinOperators.MVAPPEND);
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
      registerOperator(JSON_EXTRACT_ALL, PPLBuiltinOperators.JSON_EXTRACT_ALL); // internal

      // Register operators with a different type checker

      // Register ADD (+ symbol) for string concatenation
      // Replaced type checker since CONCAT also supports array concatenation
      registerOperator(
          ADD,
          SqlStdOperatorTable.CONCAT,
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
      // Register ADD (+ symbol) for numeric addition
      // Replace type checker since PLUS also supports binary addition
      registerOperator(
          ADD,
          SqlStdOperatorTable.PLUS,
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
      // Replace with a custom CompositeOperandTypeChecker to check both operands as
      // SqlStdOperatorTable.ITEM.getOperandTypeChecker() checks only the first
      // operand instead
      // of all operands.
      registerOperator(
          INTERNAL_ITEM,
          SqlStdOperatorTable.ITEM,
          PPLTypeChecker.wrapComposite(
              (CompositeOperandTypeChecker)
                  OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER)
                      .or(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.ANY)),
              false));
      registerOperator(
          XOR,
          SqlStdOperatorTable.NOT_EQUALS,
          PPLTypeChecker.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN));
      // SqlStdOperatorTable.CASE.getOperandTypeChecker is null. We manually create a type checker
      // for it. The second and third operands are required to be of the same type. If not,  it will
      // throw an IllegalArgumentException with information Can't find leastRestrictive type
      registerOperator(
          IF,
          SqlStdOperatorTable.CASE,
          PPLTypeChecker.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY));
      // Re-define the type checker for is not null, is present, and is null since
      // their original type checker ANY isn't compatible with struct types.
      registerOperator(
          IS_NOT_NULL,
          SqlStdOperatorTable.IS_NOT_NULL,
          PPLTypeChecker.family(SqlTypeFamily.IGNORE));
      registerOperator(
          IS_PRESENT, SqlStdOperatorTable.IS_NOT_NULL, PPLTypeChecker.family(SqlTypeFamily.IGNORE));
      registerOperator(
          IS_NULL, SqlStdOperatorTable.IS_NULL, PPLTypeChecker.family(SqlTypeFamily.IGNORE));

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
      registerOperator(
          ATAN,
          SqlStdOperatorTable.ATAN2,
          PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
      register(
          STRCMP,
          (FunctionImp2)
              (builder, arg1, arg2) -> builder.makeCall(SqlLibraryOperators.STRCMP, arg2, arg1),
          PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
      // SqlStdOperatorTable.SUBSTRING.getOperandTypeChecker is null. We manually
      // create a type
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
          ILIKE,
          (FunctionImp2)
              (builder, arg1, arg2) ->
                  builder.makeCall(
                      SqlLibraryOperators.ILIKE, arg1, arg2, builder.makeLiteral("\\")),
          PPLTypeChecker.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING));
      register(
          LIKE,
          (FunctionImp3)
              (builder, arg1, arg2, arg3) ->
                  ((RexLiteral) arg3).getValueAs(Boolean.class)
                      ? builder.makeCall(
                          SqlStdOperatorTable.LIKE, arg1, arg2, builder.makeLiteral("\\"))
                      : builder.makeCall(
                          SqlLibraryOperators.ILIKE, arg1, arg2, builder.makeLiteral("\\")),
          PPLTypeChecker.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.BOOLEAN));
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
    private static final double MEDIAN_PERCENTILE = 50.0;
    private final Map<BuiltinFunctionName, Pair<CalciteFuncSignature, AggHandler>> map =
        new HashMap<>();

    void register(
        BuiltinFunctionName functionName, AggHandler aggHandler, PPLTypeChecker typeChecker) {
      CalciteFuncSignature signature =
          new CalciteFuncSignature(functionName.getName(), typeChecker);
      map.put(functionName, Pair.of(signature, aggHandler));
    }

    void registerOperator(BuiltinFunctionName functionName, SqlAggFunction aggFunction) {
      SqlOperandTypeChecker innerTypeChecker = extractTypeCheckerFromUDF(aggFunction);
      PPLTypeChecker typeChecker =
          wrapSqlOperandTypeChecker(innerTypeChecker, functionName.name(), true);
      AggHandler handler =
          (distinct, field, argList, ctx) -> {
            List<RexNode> newArgList =
                argList.stream().map(PlanUtils::derefMapCall).collect(Collectors.toList());
            return UserDefinedFunctionUtils.makeAggregateCall(
                aggFunction, List.of(field), newArgList, ctx.relBuilder);
          };
      register(functionName, handler, typeChecker);
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

      register(
          AVG,
          (distinct, field, argList, ctx) -> ctx.relBuilder.avg(distinct, null, field),
          wrapSqlOperandTypeChecker(
              SqlStdOperatorTable.AVG.getOperandTypeChecker(), AVG.name(), false));

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
          },
          wrapSqlOperandTypeChecker(PPLOperandTypes.OPTIONAL_ANY, COUNT.name(), false));

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
          },
          wrapSqlOperandTypeChecker(
              extractTypeCheckerFromUDF(PPLBuiltinOperators.PERCENTILE_APPROX),
              PERCENTILE_APPROX.name(),
              false));

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
          },
          wrapSqlOperandTypeChecker(
              PPLOperandTypes.NUMERIC.getInnerTypeChecker(), MEDIAN.name(), false));

      register(
          EARLIEST,
          (distinct, field, argList, ctx) -> {
            List<RexNode> args = resolveTimeField(argList, ctx);
            return UserDefinedFunctionUtils.makeAggregateCall(
                SqlStdOperatorTable.ARG_MIN, List.of(field), args, ctx.relBuilder);
          },
          wrapSqlOperandTypeChecker(
              PPLOperandTypes.ANY_OPTIONAL_TIMESTAMP, EARLIEST.name(), false));

      register(
          LATEST,
          (distinct, field, argList, ctx) -> {
            List<RexNode> args = resolveTimeField(argList, ctx);
            return UserDefinedFunctionUtils.makeAggregateCall(
                SqlStdOperatorTable.ARG_MAX, List.of(field), args, ctx.relBuilder);
          },
          wrapSqlOperandTypeChecker(
              PPLOperandTypes.ANY_OPTIONAL_TIMESTAMP, EARLIEST.name(), false));

      // Register FIRST function - uses document order
      register(
          FIRST,
          (distinct, field, argList, ctx) -> {
            // Use our custom FirstAggFunction for document order aggregation
            return ctx.relBuilder.aggregateCall(PPLBuiltinOperators.FIRST, field);
          },
          wrapSqlOperandTypeChecker(
              PPLBuiltinOperators.FIRST.getOperandTypeChecker(), FIRST.name(), false));

      // Register LAST function - uses document order
      register(
          LAST,
          (distinct, field, argList, ctx) -> {
            // Use our custom LastAggFunction for document order aggregation
            return ctx.relBuilder.aggregateCall(PPLBuiltinOperators.LAST, field);
          },
          wrapSqlOperandTypeChecker(
              PPLBuiltinOperators.LAST.getOperandTypeChecker(), LAST.name(), false));
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
    if (typeChecker instanceof ImplicitCastOperandTypeChecker implicitCastTypeChecker) {
      pplTypeChecker = PPLTypeChecker.wrapFamily(implicitCastTypeChecker);
    } else if (typeChecker instanceof CompositeOperandTypeChecker compositeTypeChecker) {
      // UDFs implement their own composite type checkers, which always use OR logic for
      // argument
      // types. Verifying the composition type would require accessing a protected field in
      // CompositeOperandTypeChecker. If access to this field is not allowed, type checking will
      // be skipped, so we avoid checking the composition type here.

      // If compositeTypeChecker contains operand checkers other than family type checkers or
      // other than OR compositions, the function with be registered with a null type checker,
      // which means the function will not be type checked.
      try {
        pplTypeChecker = PPLTypeChecker.wrapComposite(compositeTypeChecker, !isUserDefinedFunction);
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
    } else if (typeChecker != null) {
      pplTypeChecker = PPLTypeChecker.wrapDefault(typeChecker);
    } else {
      logger.info(
          "Cannot create type checker for function: {}. Will skip its type checking", functionName);
      pplTypeChecker = null;
    }
    return pplTypeChecker;
  }

  /**
   * Extracts the underlying {@link SqlOperandTypeChecker} from a {@link SqlOperator}.
   *
   * <p>For user-defined functions (UDFs) and user-defined aggregate functions (UDAFs), the {@link
   * SqlOperandTypeChecker} is typically wrapped in a {@link UDFOperandMetadata}, which contains the
   * actual type checker used for operand validation. Most of these wrapped type checkers are
   * defined in {@link org.opensearch.sql.calcite.utils.PPLOperandTypes}. This method retrieves the
   * inner type checker from {@link UDFOperandMetadata} if present.
   *
   * <p>For Calcite's built-in operators, its type checker is returned directly.
   *
   * @param operator the {@link SqlOperator}, which may be a Calcite built-in operator, a
   *     user-defined function, or a user-defined aggregation function
   * @return the underlying {@link SqlOperandTypeChecker} instance, or {@code null} if not available
   */
  private static SqlOperandTypeChecker extractTypeCheckerFromUDF(SqlOperator operator) {
    SqlOperandTypeChecker typeChecker = operator.getOperandTypeChecker();
    if (typeChecker instanceof UDFOperandMetadata) {
      UDFOperandMetadata udfOperandMetadata = (UDFOperandMetadata) typeChecker;
      return udfOperandMetadata.getInnerTypeChecker();
    }
    return typeChecker;
  }
}
