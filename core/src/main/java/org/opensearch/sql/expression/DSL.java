/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import java.util.Arrays;
import java.util.Collections;
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
      return named(((ParseExpression) expression).getIdentifier().valueOf(null).stringValue(),
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
    return function(BuiltinFunctionName.ABS, expressions);
  }

  public static FunctionExpression ceil(Expression... expressions) {
    return function(BuiltinFunctionName.CEIL, expressions);
  }

  public static FunctionExpression ceiling(Expression... expressions) {
    return function(BuiltinFunctionName.CEILING, expressions);
  }

  public static FunctionExpression conv(Expression... expressions) {
    return function(BuiltinFunctionName.CONV, expressions);
  }

  public static FunctionExpression crc32(Expression... expressions) {
    return function(BuiltinFunctionName.CRC32, expressions);
  }

  public static FunctionExpression euler(Expression... expressions) {
    return function(BuiltinFunctionName.E, expressions);
  }

  public static FunctionExpression exp(Expression... expressions) {
    return function(BuiltinFunctionName.EXP, expressions);
  }

  public static FunctionExpression floor(Expression... expressions) {
    return function(BuiltinFunctionName.FLOOR, expressions);
  }

  public static FunctionExpression ln(Expression... expressions) {
    return function(BuiltinFunctionName.LN, expressions);
  }

  public static FunctionExpression log(Expression... expressions) {
    return function(BuiltinFunctionName.LOG, expressions);
  }

  public static FunctionExpression log10(Expression... expressions) {
    return function(BuiltinFunctionName.LOG10, expressions);
  }

  public static FunctionExpression log2(Expression... expressions) {
    return function(BuiltinFunctionName.LOG2, expressions);
  }

  public static FunctionExpression mod(Expression... expressions) {
    return function(BuiltinFunctionName.MOD, expressions);
  }

  public static FunctionExpression pi(Expression... expressions) {
    return function(BuiltinFunctionName.PI, expressions);
  }

  public static FunctionExpression pow(Expression... expressions) {
    return function(BuiltinFunctionName.POW, expressions);
  }

  public static FunctionExpression power(Expression... expressions) {
    return function(BuiltinFunctionName.POWER, expressions);
  }

  public static FunctionExpression rand(Expression... expressions) {
    return function(BuiltinFunctionName.RAND, expressions);
  }

  public static FunctionExpression round(Expression... expressions) {
    return function(BuiltinFunctionName.ROUND, expressions);
  }

  public static FunctionExpression sign(Expression... expressions) {
    return function(BuiltinFunctionName.SIGN, expressions);
  }

  public static FunctionExpression sqrt(Expression... expressions) {
    return function(BuiltinFunctionName.SQRT, expressions);
  }

  public static FunctionExpression truncate(Expression... expressions) {
    return function(BuiltinFunctionName.TRUNCATE, expressions);
  }

  public static FunctionExpression acos(Expression... expressions) {
    return function(BuiltinFunctionName.ACOS, expressions);
  }

  public static FunctionExpression asin(Expression... expressions) {
    return function(BuiltinFunctionName.ASIN, expressions);
  }

  public static FunctionExpression atan(Expression... expressions) {
    return function(BuiltinFunctionName.ATAN, expressions);
  }

  public static FunctionExpression atan2(Expression... expressions) {
    return function(BuiltinFunctionName.ATAN2, expressions);
  }

  public static FunctionExpression cos(Expression... expressions) {
    return function(BuiltinFunctionName.COS, expressions);
  }

  public static FunctionExpression cot(Expression... expressions) {
    return function(BuiltinFunctionName.COT, expressions);
  }

  public static FunctionExpression degrees(Expression... expressions) {
    return function(BuiltinFunctionName.DEGREES, expressions);
  }

  public static FunctionExpression radians(Expression... expressions) {
    return function(BuiltinFunctionName.RADIANS, expressions);
  }

  public static FunctionExpression sin(Expression... expressions) {
    return function(BuiltinFunctionName.SIN, expressions);
  }

  public static FunctionExpression tan(Expression... expressions) {
    return function(BuiltinFunctionName.TAN, expressions);
  }

  public static FunctionExpression add(Expression... expressions) {
    return function(BuiltinFunctionName.ADD, expressions);
  }

  public static FunctionExpression subtract(Expression... expressions) {
    return function(BuiltinFunctionName.SUBTRACT, expressions);
  }

  public static FunctionExpression multiply(Expression... expressions) {
    return function(BuiltinFunctionName.MULTIPLY, expressions);
  }

  public static FunctionExpression adddate(Expression... expressions) {
    return function(BuiltinFunctionName.ADDDATE, expressions);
  }

  public static FunctionExpression convert_tz(Expression... expressions) {
    return function(BuiltinFunctionName.CONVERT_TZ, expressions);
  }

  public static FunctionExpression date(Expression... expressions) {
    return function(BuiltinFunctionName.DATE, expressions);
  }

  public static FunctionExpression datetime(Expression... expressions) {
    return function(BuiltinFunctionName.DATETIME, expressions);
  }

  public static FunctionExpression date_add(Expression... expressions) {
    return function(BuiltinFunctionName.DATE_ADD, expressions);
  }

  public static FunctionExpression date_sub(Expression... expressions) {
    return function(BuiltinFunctionName.DATE_SUB, expressions);
  }

  public static FunctionExpression day(Expression... expressions) {
    return function(BuiltinFunctionName.DAY, expressions);
  }

  public static FunctionExpression dayname(Expression... expressions) {
    return function(BuiltinFunctionName.DAYNAME, expressions);
  }

  public static FunctionExpression dayofmonth(Expression... expressions) {
    return function(BuiltinFunctionName.DAYOFMONTH, expressions);
  }

  public static FunctionExpression dayofweek(Expression... expressions) {
    return function(BuiltinFunctionName.DAYOFWEEK, expressions);
  }

  public static FunctionExpression dayofyear(Expression... expressions) {
    return function(BuiltinFunctionName.DAYOFYEAR, expressions);
  }

  public static FunctionExpression from_days(Expression... expressions) {
    return function(BuiltinFunctionName.FROM_DAYS, expressions);
  }

  public static FunctionExpression hour(Expression... expressions) {
    return function(BuiltinFunctionName.HOUR, expressions);
  }

  public static FunctionExpression microsecond(Expression... expressions) {
    return function(BuiltinFunctionName.MICROSECOND, expressions);
  }

  public static FunctionExpression minute(Expression... expressions) {
    return function(BuiltinFunctionName.MINUTE, expressions);
  }

  public static FunctionExpression month(Expression... expressions) {
    return function(BuiltinFunctionName.MONTH, expressions);
  }

  public static FunctionExpression monthname(Expression... expressions) {
    return function(BuiltinFunctionName.MONTHNAME, expressions);
  }

  public static FunctionExpression quarter(Expression... expressions) {
    return function(BuiltinFunctionName.QUARTER, expressions);
  }

  public static FunctionExpression second(Expression... expressions) {
    return function(BuiltinFunctionName.SECOND, expressions);
  }

  public static FunctionExpression subdate(Expression... expressions) {
    return function(BuiltinFunctionName.SUBDATE, expressions);
  }

  public static FunctionExpression time(Expression... expressions) {
    return function(BuiltinFunctionName.TIME, expressions);
  }

  public static FunctionExpression time_to_sec(Expression... expressions) {
    return function(BuiltinFunctionName.TIME_TO_SEC, expressions);
  }

  public static FunctionExpression timestamp(Expression... expressions) {
    return function(BuiltinFunctionName.TIMESTAMP, expressions);
  }

  public static FunctionExpression date_format(Expression... expressions) {
    return function(BuiltinFunctionName.DATE_FORMAT, expressions);
  }

  public static FunctionExpression to_days(Expression... expressions) {
    return function(BuiltinFunctionName.TO_DAYS, expressions);
  }

  public static FunctionExpression week(Expression... expressions) {
    return function(BuiltinFunctionName.WEEK, expressions);
  }

  public static FunctionExpression year(Expression... expressions) {
    return function(BuiltinFunctionName.YEAR, expressions);
  }

  public static FunctionExpression divide(Expression... expressions) {
    return function(BuiltinFunctionName.DIVIDE, expressions);
  }

  public static FunctionExpression module(Expression... expressions) {
    return function(BuiltinFunctionName.MODULES, expressions);
  }

  public static FunctionExpression substr(Expression... expressions) {
    return function(BuiltinFunctionName.SUBSTR, expressions);
  }

  public static FunctionExpression substring(Expression... expressions) {
    return function(BuiltinFunctionName.SUBSTR, expressions);
  }

  public static FunctionExpression ltrim(Expression... expressions) {
    return function(BuiltinFunctionName.LTRIM, expressions);
  }

  public static FunctionExpression rtrim(Expression... expressions) {
    return function(BuiltinFunctionName.RTRIM, expressions);
  }

  public static FunctionExpression trim(Expression... expressions) {
    return function(BuiltinFunctionName.TRIM, expressions);
  }

  public static FunctionExpression upper(Expression... expressions) {
    return function(BuiltinFunctionName.UPPER, expressions);
  }

  public static FunctionExpression lower(Expression... expressions) {
    return function(BuiltinFunctionName.LOWER, expressions);
  }

  public static FunctionExpression regexp(Expression... expressions) {
    return function(BuiltinFunctionName.REGEXP, expressions);
  }

  public static FunctionExpression concat(Expression... expressions) {
    return function(BuiltinFunctionName.CONCAT, expressions);
  }

  public static FunctionExpression concat_ws(Expression... expressions) {
    return function(BuiltinFunctionName.CONCAT_WS, expressions);
  }

  public static FunctionExpression length(Expression... expressions) {
    return function(BuiltinFunctionName.LENGTH, expressions);
  }

  public static FunctionExpression strcmp(Expression... expressions) {
    return function(BuiltinFunctionName.STRCMP, expressions);
  }

  public static FunctionExpression right(Expression... expressions) {
    return function(BuiltinFunctionName.RIGHT, expressions);
  }

  public static FunctionExpression left(Expression... expressions) {
    return function(BuiltinFunctionName.LEFT, expressions);
  }

  public static FunctionExpression ascii(Expression... expressions) {
    return function(BuiltinFunctionName.ASCII, expressions);
  }

  public static FunctionExpression locate(Expression... expressions) {
    return function(BuiltinFunctionName.LOCATE, expressions);
  }

  public static FunctionExpression replace(Expression... expressions) {
    return function(BuiltinFunctionName.REPLACE, expressions);
  }

  public static FunctionExpression and(Expression... expressions) {
    return function(BuiltinFunctionName.AND, expressions);
  }

  public static FunctionExpression or(Expression... expressions) {
    return function(BuiltinFunctionName.OR, expressions);
  }

  public static FunctionExpression xor(Expression... expressions) {
    return function(BuiltinFunctionName.XOR, expressions);
  }

  public static FunctionExpression not(Expression... expressions) {
    return function(BuiltinFunctionName.NOT, expressions);
  }

  public static FunctionExpression equal(Expression... expressions) {
    return function(BuiltinFunctionName.EQUAL, expressions);
  }

  public static FunctionExpression notequal(Expression... expressions) {
    return function(BuiltinFunctionName.NOTEQUAL, expressions);
  }

  public static FunctionExpression less(Expression... expressions) {
    return function(BuiltinFunctionName.LESS, expressions);
  }

  public static FunctionExpression lte(Expression... expressions) {
    return function(BuiltinFunctionName.LTE, expressions);
  }

  public static FunctionExpression greater(Expression... expressions) {
    return function(BuiltinFunctionName.GREATER, expressions);
  }

  public static FunctionExpression gte(Expression... expressions) {
    return function(BuiltinFunctionName.GTE, expressions);
  }

  public static FunctionExpression like(Expression... expressions) {
    return function(BuiltinFunctionName.LIKE, expressions);
  }

  public static FunctionExpression notLike(Expression... expressions) {
    return function(BuiltinFunctionName.NOT_LIKE, expressions);
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
    return (RankingWindowFunction) BuiltinFunctionRepository.getInstance().compile(
        BuiltinFunctionName.ROW_NUMBER.getName(), Collections.emptyList());
  }

  public static RankingWindowFunction rank() {
    return (RankingWindowFunction) BuiltinFunctionRepository.getInstance().compile(
        BuiltinFunctionName.RANK.getName(), Collections.emptyList());
  }

  public static RankingWindowFunction denseRank() {
    return (RankingWindowFunction) BuiltinFunctionRepository.getInstance().compile(
        BuiltinFunctionName.DENSE_RANK.getName(), Collections.emptyList());
  }

  public static Aggregator min(Expression... expressions) {
    return aggregate(BuiltinFunctionName.MIN, expressions);
  }

  public static Aggregator max(Expression... expressions) {
    return aggregate(BuiltinFunctionName.MAX, expressions);
  }

  private static FunctionExpression function(BuiltinFunctionName functionName,
                                             Expression... expressions) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance().compile(
        functionName.getName(), Arrays.asList(expressions));
  }

  private static Aggregator aggregate(BuiltinFunctionName functionName, Expression... expressions) {
    return (Aggregator) BuiltinFunctionRepository.getInstance().compile(
        functionName.getName(), Arrays.asList(expressions));
  }

  public static FunctionExpression isnull(Expression... expressions) {
    return function(BuiltinFunctionName.ISNULL, expressions);
  }

  public static FunctionExpression is_null(Expression... expressions) {
    return function(BuiltinFunctionName.IS_NULL, expressions);
  }

  public static FunctionExpression isnotnull(Expression... expressions) {
    return function(BuiltinFunctionName.IS_NOT_NULL, expressions);
  }

  public static FunctionExpression ifnull(Expression... expressions) {
    return function(BuiltinFunctionName.IFNULL, expressions);
  }

  public static FunctionExpression nullif(Expression... expressions) {
    return function(BuiltinFunctionName.NULLIF, expressions);
  }

  public static FunctionExpression iffunction(Expression... expressions) {
    return function(BuiltinFunctionName.IF, expressions);
  }

  public static Expression cases(Expression defaultResult,
                                 WhenClause... whenClauses) {
    return new CaseClause(Arrays.asList(whenClauses), defaultResult);
  }

  public static WhenClause when(Expression condition, Expression result) {
    return new WhenClause(condition, result);
  }

  public static FunctionExpression interval(Expression value, Expression unit) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance().compile(
        BuiltinFunctionName.INTERVAL.getName(), Arrays.asList(value, unit));
  }

  public static FunctionExpression castString(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_STRING.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castByte(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_BYTE.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castShort(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_SHORT.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castInt(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_INT.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castLong(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_LONG.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castFloat(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_FLOAT.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castDouble(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_DOUBLE.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castBoolean(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_BOOLEAN.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castDate(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_DATE.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castTime(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_TIME.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castTimestamp(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(), Arrays.asList(value));
  }

  public static FunctionExpression castDatetime(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.CAST_TO_DATETIME.getName(), Arrays.asList(value));
  }

  public static FunctionExpression typeof(Expression value) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(BuiltinFunctionName.TYPEOF.getName(), Arrays.asList(value));
  }

  public static FunctionExpression match(Expression... args) {
    return compile(BuiltinFunctionName.MATCH, args);
  }

  public static FunctionExpression match_phrase(Expression... args) {
    return compile(BuiltinFunctionName.MATCH_PHRASE, args);
  }

  public static FunctionExpression match_phrase_prefix(Expression... args) {
    return compile(BuiltinFunctionName.MATCH_PHRASE_PREFIX, args);
  }

  public static FunctionExpression multi_match(Expression... args) {
    return compile(BuiltinFunctionName.MULTI_MATCH, args);
  }

  public static FunctionExpression simple_query_string(Expression... args) {
    return compile(BuiltinFunctionName.SIMPLE_QUERY_STRING, args);
  }

  public static FunctionExpression query_string(Expression... args) {
    return compile(BuiltinFunctionName.QUERY_STRING, args);
  }

  public static FunctionExpression match_bool_prefix(Expression... args) {
    return compile(BuiltinFunctionName.MATCH_BOOL_PREFIX, args);
  }

  public static FunctionExpression now(Expression... args) {
    return compile(BuiltinFunctionName.NOW, args);
  }

  public static FunctionExpression current_timestamp(Expression... args) {
    return compile(BuiltinFunctionName.CURRENT_TIMESTAMP, args);
  }

  public static FunctionExpression localtimestamp(Expression... args) {
    return compile(BuiltinFunctionName.LOCALTIMESTAMP, args);
  }

  public static FunctionExpression localtime(Expression... args) {
    return compile(BuiltinFunctionName.LOCALTIME, args);
  }

  public static FunctionExpression sysdate(Expression... args) {
    return compile(BuiltinFunctionName.SYSDATE, args);
  }

  public static FunctionExpression curtime(Expression... args) {
    return compile(BuiltinFunctionName.CURTIME, args);
  }

  public static FunctionExpression current_time(Expression... args) {
    return compile(BuiltinFunctionName.CURRENT_TIME, args);
  }

  public static FunctionExpression curdate(Expression... args) {
    return compile(BuiltinFunctionName.CURDATE, args);
  }

  public static FunctionExpression current_date(Expression... args) {
    return compile(BuiltinFunctionName.CURRENT_DATE, args);
  }

  private static FunctionExpression compile(BuiltinFunctionName bfn, Expression... args) {
    return (FunctionExpression) BuiltinFunctionRepository.getInstance()
        .compile(bfn.getName(), Arrays.asList(args.clone()));
  }
}
