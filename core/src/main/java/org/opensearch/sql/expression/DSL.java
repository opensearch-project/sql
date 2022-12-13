/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import java.util.Arrays;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.conditional.cases.CaseClause;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionImplementation;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.parse.GrokExpression;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.parse.RegexExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.expression.window.ranking.RankingWindowFunction;

public class DSL {

  private DSL() {
  }

  public static LiteralExpression literal(Byte value) {
    return new LiteralExpression(ExprValueUtils.byteValue(value));
  }

  public static LiteralExpression literal(Short value) {
    return new LiteralExpression(new ExprShortValue(value));
  }

  public static LiteralExpression literal(Integer value) {
    return new LiteralExpression(ExprValueUtils.integerValue(value));
  }

  public static LiteralExpression literal(Long value) {
    return new LiteralExpression(ExprValueUtils.longValue(value));
  }

  public static LiteralExpression literal(Float value) {
    return new LiteralExpression(ExprValueUtils.floatValue(value));
  }

  public static LiteralExpression literal(Double value) {
    return new LiteralExpression(ExprValueUtils.doubleValue(value));
  }

  public static LiteralExpression literal(String value) {
    return new LiteralExpression(ExprValueUtils.stringValue(value));
  }

  public static LiteralExpression literal(Boolean value) {
    return new LiteralExpression(ExprValueUtils.booleanValue(value));
  }

  public static LiteralExpression literal(ExprValue value) {
    return new LiteralExpression(value);
  }

  /**
   * Wrap a number to {@link LiteralExpression}.
   */
  public static LiteralExpression literal(Number value) {
    if (value instanceof Integer) {
      return new LiteralExpression(ExprValueUtils.integerValue(value.intValue()));
    } else if (value instanceof Long) {
      return new LiteralExpression(ExprValueUtils.longValue(value.longValue()));
    } else if (value instanceof Float) {
      return new LiteralExpression(ExprValueUtils.floatValue(value.floatValue()));
    } else {
      return new LiteralExpression(ExprValueUtils.doubleValue(value.doubleValue()));
    }
  }

  public static ReferenceExpression ref(String ref, ExprType type) {
    return new ReferenceExpression(ref, type);
  }

  /**
   * Wrap a named expression if not yet. The intent is that different languages may use
   * Alias or not when building AST. This caused either named or unnamed expression
   * is resolved by analyzer. To make unnamed expression acceptable for logical project,
   * it is required to wrap it by named expression here before passing to logical project.
   *
   * @param expression  expression
   * @return            expression if named already or expression wrapped by named expression.
   */
  public static NamedExpression named(Expression expression) {
    if (expression instanceof NamedExpression) {
      return (NamedExpression) expression;
    }
    if (expression instanceof ParseExpression) {
      return named(((ParseExpression) expression).getIdentifier().valueOf().stringValue(),
          expression);
    }
    return named(expression.toString(), expression);
  }

  public static NamedExpression named(String name, Expression expression) {
    return new NamedExpression(name, expression);
  }

  public static NamedExpression named(String name, Expression expression, String alias) {
    return new NamedExpression(name, expression, alias);
  }

  public static NamedAggregator named(String name, Aggregator aggregator) {
    return new NamedAggregator(name, aggregator);
  }

  public static NamedArgumentExpression namedArgument(String argName, Expression value) {
    return new NamedArgumentExpression(argName, value);
  }

  public static NamedArgumentExpression namedArgument(String name, String value) {
    return namedArgument(name, literal(value));
  }

  public static GrokExpression grok(Expression sourceField, Expression pattern,
                                    Expression identifier) {
    return new GrokExpression(sourceField, pattern, identifier);
  }

  public static RegexExpression regex(Expression sourceField, Expression pattern,
                                      Expression identifier) {
    return new RegexExpression(sourceField, pattern, identifier);
  }

  public static PatternsExpression patterns(Expression sourceField, Expression pattern,
                                            Expression identifier) {
    return new PatternsExpression(sourceField, pattern, identifier);
  }

  public static SpanExpression span(Expression field, Expression value, String unit) {
    return new SpanExpression(field, value, SpanUnit.of(unit));
  }

  public static FunctionExpression abs(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ABS, expressions);
  }

  public static FunctionExpression ceil(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CEIL, expressions);
  }

  public static FunctionExpression ceiling(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CEILING, expressions);
  }

  public static FunctionExpression conv(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CONV, expressions);
  }

  public static FunctionExpression crc32(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CRC32, expressions);
  }

  public static FunctionExpression euler(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.E, expressions);
  }

  public static FunctionExpression exp(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.EXP, expressions);
  }

  public static FunctionExpression floor(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.FLOOR, expressions);
  }

  public static FunctionExpression ln(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LN, expressions);
  }

  public static FunctionExpression log(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LOG, expressions);
  }

  public static FunctionExpression log10(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LOG10, expressions);
  }

  public static FunctionExpression log2(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LOG2, expressions);
  }

  public static FunctionExpression mod(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MOD, expressions);
  }

  public static FunctionExpression pi(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.PI, expressions);
  }

  public static FunctionExpression pow(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.POW, expressions);
  }

  public static FunctionExpression power(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.POWER, expressions);
  }

  public static FunctionExpression rand(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.RAND, expressions);
  }

  public static FunctionExpression round(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ROUND, expressions);
  }

  public static FunctionExpression sign(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SIGN, expressions);
  }

  public static FunctionExpression sqrt(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SQRT, expressions);
  }

  public static FunctionExpression cbrt(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CBRT, expressions);
  }

  public static FunctionExpression position(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.POSITION, expressions);
  }

  public static FunctionExpression truncate(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TRUNCATE, expressions);
  }

  public static FunctionExpression acos(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ACOS, expressions);
  }

  public static FunctionExpression asin(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ASIN, expressions);
  }

  public static FunctionExpression atan(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ATAN, expressions);
  }

  public static FunctionExpression atan2(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ATAN2, expressions);
  }

  public static FunctionExpression cos(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.COS, expressions);
  }

  public static FunctionExpression cot(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.COT, expressions);
  }

  public static FunctionExpression degrees(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DEGREES, expressions);
  }

  public static FunctionExpression radians(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.RADIANS, expressions);
  }

  public static FunctionExpression sin(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SIN, expressions);
  }

  public static FunctionExpression tan(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TAN, expressions);
  }

  public static FunctionExpression add(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ADD, expressions);
  }

  public static FunctionExpression subtract(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SUBTRACT, expressions);
  }

  public static FunctionExpression multiply(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MULTIPLY, expressions);
  }

  public static FunctionExpression adddate(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ADDDATE, expressions);
  }

  public static FunctionExpression convert_tz(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CONVERT_TZ, expressions);
  }

  public static FunctionExpression date(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DATE, expressions);
  }

  public static FunctionExpression datetime(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DATETIME, expressions);
  }

  public static FunctionExpression date_add(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DATE_ADD, expressions);
  }

  public static FunctionExpression date_sub(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DATE_SUB, expressions);
  }

  public static FunctionExpression day(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DAY, expressions);
  }

  public static FunctionExpression dayname(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DAYNAME, expressions);
  }

  public static FunctionExpression dayofmonth(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DAYOFMONTH, expressions);
  }

  public static FunctionExpression dayofweek(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DAYOFWEEK, expressions);
  }

  public static FunctionExpression dayofyear(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DAYOFYEAR, expressions);
  }

  public static FunctionExpression day_of_year(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DAY_OF_YEAR, expressions);
  }

  public static FunctionExpression from_days(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.FROM_DAYS, expressions);
  }

  public static FunctionExpression hour(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.HOUR, expressions);
  }

  public static FunctionExpression microsecond(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MICROSECOND, expressions);
  }

  public static FunctionExpression minute(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MINUTE, expressions);
  }

  public static FunctionExpression month(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MONTH, expressions);
  }

  public static FunctionExpression month_of_year(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MONTH_OF_YEAR, expressions);
  }

  public static FunctionExpression monthname(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MONTHNAME, expressions);
  }

  public static FunctionExpression quarter(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.QUARTER, expressions);
  }

  public static FunctionExpression second(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SECOND, expressions);
  }

  public static FunctionExpression subdate(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SUBDATE, expressions);
  }

  public static FunctionExpression time(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TIME, expressions);
  }

  public static FunctionExpression time_to_sec(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TIME_TO_SEC, expressions);
  }

  public static FunctionExpression timestamp(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TIMESTAMP, expressions);
  }

  public static FunctionExpression date_format(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DATE_FORMAT, expressions);
  }

  public static FunctionExpression to_days(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TO_DAYS, expressions);
  }

  public static FunctionExpression week(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.WEEK, expressions);
  }

  public static FunctionExpression week_of_year(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.WEEK_OF_YEAR, expressions);
  }

  public static FunctionExpression year(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.YEAR, expressions);
  }

  public static FunctionExpression divide(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.DIVIDE, expressions);
  }

  public static FunctionExpression module(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MODULES, expressions);
  }

  public static FunctionExpression substr(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SUBSTR, expressions);
  }

  public static FunctionExpression substring(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SUBSTR, expressions);
  }

  public static FunctionExpression ltrim(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LTRIM, expressions);
  }

  public static FunctionExpression rtrim(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.RTRIM, expressions);
  }

  public static FunctionExpression trim(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TRIM, expressions);
  }

  public static FunctionExpression upper(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.UPPER, expressions);
  }

  public static FunctionExpression lower(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LOWER, expressions);
  }

  public static FunctionExpression regexp(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.REGEXP, expressions);
  }

  public static FunctionExpression concat(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CONCAT, expressions);
  }

  public static FunctionExpression concat_ws(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CONCAT_WS, expressions);
  }

  public static FunctionExpression length(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LENGTH, expressions);
  }

  public static FunctionExpression strcmp(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.STRCMP, expressions);
  }

  public static FunctionExpression right(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.RIGHT, expressions);
  }

  public static FunctionExpression left(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LEFT, expressions);
  }

  public static FunctionExpression ascii(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ASCII, expressions);
  }

  public static FunctionExpression locate(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LOCATE, expressions);
  }

  public static FunctionExpression replace(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.REPLACE, expressions);
  }

  public static FunctionExpression reverse(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.REVERSE, expressions);
  }

  public static FunctionExpression and(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.AND, expressions);
  }

  public static FunctionExpression or(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.OR, expressions);
  }

  public static FunctionExpression xor(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.XOR, expressions);
  }

  public static FunctionExpression not(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.NOT, expressions);
  }

  public static FunctionExpression equal(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.EQUAL, expressions);
  }

  public static FunctionExpression notequal(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.NOTEQUAL, expressions);
  }

  public static FunctionExpression less(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LESS, expressions);
  }

  public static FunctionExpression lte(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LTE, expressions);
  }

  public static FunctionExpression greater(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.GREATER, expressions);
  }

  public static FunctionExpression gte(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.GTE, expressions);
  }

  public static FunctionExpression like(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.LIKE, expressions);
  }

  public static FunctionExpression notLike(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.NOT_LIKE, expressions);
  }

  public static Aggregator avg(Expression... expressions) {
    return aggregate(BuiltinFunctionName.AVG, expressions);
  }

  public static Aggregator sum(Expression... expressions) {
    return aggregate(BuiltinFunctionName.SUM, expressions);
  }

  public static Aggregator count(Expression... expressions) {
    return aggregate(BuiltinFunctionName.COUNT, expressions);
  }

  public static Aggregator distinctCount(Expression... expressions) {
    return count(expressions).distinct(true);
  }

  public static Aggregator varSamp(Expression... expressions) {
    return aggregate(BuiltinFunctionName.VARSAMP, expressions);
  }

  public static Aggregator varPop(Expression... expressions) {
    return aggregate(BuiltinFunctionName.VARPOP, expressions);
  }

  public static Aggregator stddevSamp(Expression... expressions) {
    return aggregate(BuiltinFunctionName.STDDEV_SAMP, expressions);
  }

  public static Aggregator stddevPop(Expression... expressions) {
    return aggregate(BuiltinFunctionName.STDDEV_POP, expressions);
  }

  public static Aggregator take(Expression... expressions) {
    return aggregate(BuiltinFunctionName.TAKE, expressions);
  }

  public static RankingWindowFunction rowNumber() {
    return compile(FunctionProperties.None, BuiltinFunctionName.ROW_NUMBER);
  }

  public static RankingWindowFunction rank() {
    return compile(FunctionProperties.None, BuiltinFunctionName.RANK);
  }

  public static RankingWindowFunction denseRank() {
    return compile(FunctionProperties.None, BuiltinFunctionName.DENSE_RANK);
  }

  public static Aggregator min(Expression... expressions) {
    return aggregate(BuiltinFunctionName.MIN, expressions);
  }

  public static Aggregator max(Expression... expressions) {
    return aggregate(BuiltinFunctionName.MAX, expressions);
  }

  private static Aggregator aggregate(BuiltinFunctionName functionName, Expression... expressions) {
    return compile(FunctionProperties.None, functionName, expressions);
  }

  public static FunctionExpression isnull(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.ISNULL, expressions);
  }

  public static FunctionExpression is_null(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.IS_NULL, expressions);
  }

  public static FunctionExpression isnotnull(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.IS_NOT_NULL, expressions);
  }

  public static FunctionExpression ifnull(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.IFNULL, expressions);
  }

  public static FunctionExpression nullif(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.NULLIF, expressions);
  }

  public static FunctionExpression iffunction(Expression... expressions) {
    return compile(FunctionProperties.None, BuiltinFunctionName.IF, expressions);
  }

  public static Expression cases(Expression defaultResult,
                                 WhenClause... whenClauses) {
    return new CaseClause(Arrays.asList(whenClauses), defaultResult);
  }

  public static WhenClause when(Expression condition, Expression result) {
    return new WhenClause(condition, result);
  }

  public static FunctionExpression interval(Expression value, Expression unit) {
    return compile(FunctionProperties.None, BuiltinFunctionName.INTERVAL, value, unit);
  }

  public static FunctionExpression castString(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_STRING, value);
  }

  public static FunctionExpression castByte(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_BYTE, value);
  }

  public static FunctionExpression castShort(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_SHORT, value);
  }

  public static FunctionExpression castInt(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_INT, value);
  }

  public static FunctionExpression castLong(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_LONG, value);
  }

  public static FunctionExpression castFloat(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_FLOAT, value);
  }

  public static FunctionExpression castDouble(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_DOUBLE, value);
  }

  public static FunctionExpression castBoolean(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_BOOLEAN, value);
  }

  public static FunctionExpression castDate(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_DATE, value);
  }

  public static FunctionExpression castTime(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_TIME, value);
  }

  public static FunctionExpression castTimestamp(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_TIMESTAMP, value);
  }

  public static FunctionExpression castDatetime(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.CAST_TO_DATETIME, value);
  }

  public static FunctionExpression typeof(Expression value) {
    return compile(FunctionProperties.None, BuiltinFunctionName.TYPEOF, value);
  }

  public static FunctionExpression match(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MATCH, args);
  }

  public static FunctionExpression match_phrase(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MATCH_PHRASE, args);
  }

  public static FunctionExpression match_phrase_prefix(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MATCH_PHRASE_PREFIX, args);
  }

  public static FunctionExpression multi_match(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MULTI_MATCH, args);
  }

  public static FunctionExpression simple_query_string(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.SIMPLE_QUERY_STRING, args);
  }

  public static FunctionExpression query(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.QUERY, args);
  }

  public static FunctionExpression query_string(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.QUERY_STRING, args);
  }

  public static FunctionExpression match_bool_prefix(Expression... args) {
    return compile(FunctionProperties.None, BuiltinFunctionName.MATCH_BOOL_PREFIX, args);
  }

  public static FunctionExpression wildcard_query(Expression... args) {
    return compile(FunctionProperties.None,BuiltinFunctionName.WILDCARD_QUERY, args);
  }

  public static FunctionExpression now(FunctionProperties functionProperties,
                                       Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.NOW, args);
  }

  public static FunctionExpression current_timestamp(FunctionProperties functionProperties,
                                                     Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.CURRENT_TIMESTAMP, args);
  }

  public static FunctionExpression localtimestamp(FunctionProperties functionProperties,
                                                  Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.LOCALTIMESTAMP, args);
  }

  public static FunctionExpression localtime(FunctionProperties functionProperties,
                                             Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.LOCALTIME, args);
  }

  public static FunctionExpression sysdate(FunctionProperties functionProperties,
                                           Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.SYSDATE, args);
  }

  public static FunctionExpression curtime(FunctionProperties functionProperties,
                                           Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.CURTIME, args);
  }

  public static FunctionExpression current_time(FunctionProperties functionProperties,
                                                Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.CURRENT_TIME, args);
  }

  public static FunctionExpression curdate(FunctionProperties functionProperties,
                                           Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.CURDATE, args);
  }

  public static FunctionExpression current_date(FunctionProperties functionProperties,
                                                Expression... args) {
    return compile(functionProperties, BuiltinFunctionName.CURRENT_DATE, args);
  }

  @SuppressWarnings("unchecked")
  private static <T extends FunctionImplementation>
      T compile(FunctionProperties functionProperties,
                BuiltinFunctionName bfn, Expression... args) {
    return (T) BuiltinFunctionRepository.getInstance().compile(functionProperties,
        bfn.getName(), Arrays.asList(args));
  }
}
