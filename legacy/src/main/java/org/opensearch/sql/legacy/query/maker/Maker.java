/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.maker;

import static org.opensearch.sql.legacy.parser.WhereParser.getConditionForMethod;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.geo.parsers.ShapeParser;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.GeoPolygonQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpFlag;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.join.query.JoinQueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.Paramer;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.parser.ScriptFilter;
import org.opensearch.sql.legacy.parser.SubQueryExpression;
import org.opensearch.sql.legacy.spatial.BoundingBoxFilterParams;
import org.opensearch.sql.legacy.spatial.DistanceFilterParams;
import org.opensearch.sql.legacy.spatial.Point;
import org.opensearch.sql.legacy.spatial.PolygonFilterParams;
import org.opensearch.sql.legacy.spatial.WktToGeoJsonConverter;
import org.opensearch.sql.legacy.utils.StringUtils;

public abstract class Maker {

  /** UTC. */
  private static final ZoneId UTC = ZoneId.of("UTC");

  public static final Object NONE = new Object();

  public static final Set<String> queryFunctions =
      Sets.newHashSet(
          "query",
          "matchquery",
          "match_query", // match
          "multimatchquery",
          "multi_match",
          "multimatch", // multi-match
          "score",
          "scorequery",
          "score_query", // score
          "wildcardquery",
          "wildcard_query", // wildcard
          "matchphrasequery",
          "match_phrase",
          "matchphrase" // match-phrase
          );

  private static final Set<Condition.OPERATOR> NOT_OPERATOR_SET =
      ImmutableSet.of(
          Condition.OPERATOR.N,
          Condition.OPERATOR.NIN,
          Condition.OPERATOR.ISN,
          Condition.OPERATOR.NBETWEEN,
          Condition.OPERATOR.NLIKE,
          Condition.OPERATOR.NIN_TERMS,
          Condition.OPERATOR.NTERM,
          Condition.OPERATOR.NOT_EXISTS_NESTED_COMPLEX,
          Condition.OPERATOR.NREGEXP);

  protected Maker(Boolean isQuery) {}

  /**
   * @param cond
   * @return
   * @throws SqlParseException
   */
  protected ToXContent make(Condition cond) throws SqlParseException {

    String name = cond.getName();
    Object value = cond.getValue();

    ToXContent toXContent = null;

    if (value instanceof SQLMethodInvokeExpr) {
      toXContent = make(cond, name, (SQLMethodInvokeExpr) value);
    } else if (value instanceof SubQueryExpression) {
      toXContent = make(cond, name, ((SubQueryExpression) value).getValues());
    } else {
      if (cond.getValue() == NONE) {
        toXContent = new MatchNoneQueryBuilder();
      } else {
        toXContent = make(cond, name, value);
      }
    }

    return toXContent;
  }

  private ToXContent make(Condition cond, String name, SQLMethodInvokeExpr value)
      throws SqlParseException {
    ToXContent bqb = null;
    Paramer paramer = null;
    switch (value.getMethodName().toLowerCase()) {
      case "query":
        paramer = Paramer.parseParamer(value);
        QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(paramer.value);
        bqb = Paramer.fullParamer(queryString, paramer);
        bqb = applyNot(cond.getOPERATOR(), bqb);
        break;
      case "matchquery":
      case "match_query":
        paramer = Paramer.parseParamer(value);
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(name, paramer.value);
        bqb = Paramer.fullParamer(matchQuery, paramer);
        bqb = applyNot(cond.getOPERATOR(), bqb);
        break;
      case "score":
      case "scorequery":
      case "score_query":
        Float boost = Float.parseFloat(value.getParameters().get(1).toString());
        Condition subCond = getConditionForMethod(value.getParameters().get(0), cond.getConn());
        QueryBuilder subQuery = (QueryBuilder) make(subCond);
        if (subCond.isNested()) {
          subQuery = QueryBuilders.nestedQuery(subCond.getNestedPath(), subQuery, ScoreMode.None);
        }
        bqb = QueryBuilders.constantScoreQuery(subQuery).boost(boost);
        break;
      case "wildcardquery":
      case "wildcard_query":
        paramer = Paramer.parseParamer(value);
        WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(name, paramer.value);
        bqb = Paramer.fullParamer(wildcardQuery, paramer);
        break;

      case "matchphrasequery":
      case "match_phrase":
      case "matchphrase":
        paramer = Paramer.parseParamer(value);
        MatchPhraseQueryBuilder matchPhraseQuery =
            QueryBuilders.matchPhraseQuery(name, paramer.value);
        bqb = Paramer.fullParamer(matchPhraseQuery, paramer);
        break;

      case "multimatchquery":
      case "multi_match":
      case "multimatch":
        paramer = Paramer.parseParamer(value);
        MultiMatchQueryBuilder multiMatchQuery =
            QueryBuilders.multiMatchQuery(paramer.value).fields(paramer.fieldsBoosts);
        bqb = Paramer.fullParamer(multiMatchQuery, paramer);
        break;
      default:
        throw new SqlParseException(
            "The following query method is not supported: " + value.getMethodName());
    }

    return bqb;
  }

  private ToXContent make(Condition cond, String name, Object value) throws SqlParseException {
    ToXContent toXContent = null;
    switch (cond.getOPERATOR()) {
      case ISN:
      case IS:
      case N:
      case EQ:
        if (value == null || value instanceof SQLIdentifierExpr) {
          // todo: change to exists
          if (value == null || ((SQLIdentifierExpr) value).getName().equalsIgnoreCase("missing")) {
            toXContent = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(name));
          } else {
            throw new SqlParseException(
                String.format(
                    "Cannot recoginze Sql identifer %s", ((SQLIdentifierExpr) value).getName()));
          }
          break;
        } else {
          toXContent = QueryBuilders.termQuery(name, value);
          break;
        }
      case LIKE:
      case NLIKE:
        String queryStr = ((String) value);
        queryStr = queryStr.replace('%', '*').replace('_', '?');
        queryStr = queryStr.replace("&PERCENT", "%").replace("&UNDERSCORE", "_");
        toXContent = QueryBuilders.wildcardQuery(name, queryStr);
        break;
      case REGEXP:
      case NREGEXP:
        Object[] values = (Object[]) value;
        RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery(name, values[0].toString());
        if (1 < values.length) {
          String[] flags = values[1].toString().split("\\|");
          RegexpFlag[] regexpFlags = new RegexpFlag[flags.length];
          for (int i = 0; i < flags.length; ++i) {
            regexpFlags[i] = RegexpFlag.valueOf(flags[i]);
          }
          regexpQuery.flags(regexpFlags);
        }
        if (2 < values.length) {
          regexpQuery.maxDeterminizedStates(Integer.parseInt(values[2].toString()));
        }
        toXContent = regexpQuery;
        break;
      case GT:
        toXContent = QueryBuilders.rangeQuery(name).gt(value);
        break;
      case GTE:
        toXContent = QueryBuilders.rangeQuery(name).gte(value);
        break;
      case LT:
        toXContent = QueryBuilders.rangeQuery(name).lt(value);
        break;
      case LTE:
        toXContent = QueryBuilders.rangeQuery(name).lte(value);
        break;
      case NIN:
      case IN:
        // todo: value is subquery? here or before
        values = (Object[]) value;
        TermQueryBuilder[] termQueries = new TermQueryBuilder[values.length];
        for (int i = 0; i < values.length; i++) {
          termQueries[i] = QueryBuilders.termQuery(name, values[i]);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (TermQueryBuilder termQuery : termQueries) {
          boolQuery.should(termQuery);
        }
        toXContent = boolQuery;
        break;
      case BETWEEN:
      case NBETWEEN:
        toXContent =
            QueryBuilders.rangeQuery(name).gte(((Object[]) value)[0]).lte(((Object[]) value)[1]);
        break;
      case GEO_INTERSECTS:
        String wkt = cond.getValue().toString();
        try {
          ShapeBuilder shapeBuilder = getShapeBuilderFromString(wkt);
          toXContent = QueryBuilders.geoShapeQuery(cond.getName(), shapeBuilder);
        } catch (IOException e) {
          e.printStackTrace();
          throw new SqlParseException(
              StringUtils.format("Failed to create shapeBuilder from [%s]", wkt));
        }
        break;
      case GEO_BOUNDING_BOX:
        BoundingBoxFilterParams boxFilterParams = (BoundingBoxFilterParams) cond.getValue();
        Point topLeft = boxFilterParams.getTopLeft();
        Point bottomRight = boxFilterParams.getBottomRight();
        toXContent =
            QueryBuilders.geoBoundingBoxQuery(cond.getName())
                .setCorners(
                    topLeft.getLat(), topLeft.getLon(), bottomRight.getLat(), bottomRight.getLon());
        break;
      case GEO_DISTANCE:
        DistanceFilterParams distanceFilterParams = (DistanceFilterParams) cond.getValue();
        Point fromPoint = distanceFilterParams.getFrom();
        String distance = trimApostrophes(distanceFilterParams.getDistance());
        toXContent =
            QueryBuilders.geoDistanceQuery(cond.getName())
                .distance(distance)
                .point(fromPoint.getLat(), fromPoint.getLon());
        break;
      case GEO_POLYGON:
        PolygonFilterParams polygonFilterParams = (PolygonFilterParams) cond.getValue();
        ArrayList<GeoPoint> geoPoints = new ArrayList<GeoPoint>();
        for (Point p : polygonFilterParams.getPolygon()) {
          geoPoints.add(new GeoPoint(p.getLat(), p.getLon()));
        }
        GeoPolygonQueryBuilder polygonFilterBuilder =
            QueryBuilders.geoPolygonQuery(cond.getName(), geoPoints);
        toXContent = polygonFilterBuilder;
        break;
      case NIN_TERMS:
      case IN_TERMS:
        Object[] termValues = (Object[]) value;
        if (termValues.length == 1 && termValues[0] instanceof SubQueryExpression) {
          termValues = ((SubQueryExpression) termValues[0]).getValues();
        }
        Object[] termValuesObjects = new Object[termValues.length];
        for (int i = 0; i < termValues.length; i++) {
          termValuesObjects[i] = parseTermValue(termValues[i]);
        }
        toXContent = QueryBuilders.termsQuery(name, termValuesObjects);
        break;
      case NTERM:
      case TERM:
        Object term = ((Object[]) value)[0];
        toXContent = QueryBuilders.termQuery(name, parseTermValue(term));
        break;
      case IDS_QUERY:
        Object[] idsParameters = (Object[]) value;
        String[] ids;
        if (idsParameters.length == 2 && idsParameters[1] instanceof SubQueryExpression) {
          Object[] idsFromSubQuery = ((SubQueryExpression) idsParameters[1]).getValues();
          ids = arrayOfObjectsToStringArray(idsFromSubQuery, 0, idsFromSubQuery.length - 1);
        } else {
          ids = arrayOfObjectsToStringArray(idsParameters, 1, idsParameters.length - 1);
        }
        toXContent = QueryBuilders.idsQuery().addIds(ids);
        break;
      case NESTED_COMPLEX:
      case NOT_EXISTS_NESTED_COMPLEX:
        if (value == null || !(value instanceof Where)) {
          throw new SqlParseException("unsupported nested condition");
        }

        Where whereNested = (Where) value;
        BoolQueryBuilder nestedFilter = QueryMaker.explain(whereNested);

        toXContent = QueryBuilders.nestedQuery(name, nestedFilter, ScoreMode.None);
        break;
      case CHILDREN_COMPLEX:
        if (value == null || !(value instanceof Where)) {
          throw new SqlParseException("unsupported nested condition");
        }

        Where whereChildren = (Where) value;
        BoolQueryBuilder childrenFilter = QueryMaker.explain(whereChildren);
        // todo: pass score mode
        toXContent = JoinQueryBuilders.hasChildQuery(name, childrenFilter, ScoreMode.None);

        break;
      case SCRIPT:
        ScriptFilter scriptFilter = (ScriptFilter) value;
        Map<String, Object> params = new HashMap<>();
        if (scriptFilter.containsParameters()) {
          params = scriptFilter.getArgs();
        }

        SQLExpr nameExpr = cond.getNameExpr();
        SQLExpr valueExpr = cond.getValueExpr();
        if (nameExpr instanceof SQLMethodInvokeExpr
            && ((SQLMethodInvokeExpr) nameExpr).getMethodName().equalsIgnoreCase("date_format")) {
          toXContent = makeForDateFormat((SQLMethodInvokeExpr) nameExpr, (SQLCharExpr) valueExpr);
        } else {
          toXContent =
              QueryBuilders.scriptQuery(
                  new Script(
                      scriptFilter.getScriptType(),
                      Script.DEFAULT_SCRIPT_LANG,
                      scriptFilter.getScript(),
                      params));
        }
        break;
      default:
        throw new SqlParseException("Undefined condition:  " + cond.getName());
    }

    toXContent = applyNot(cond.getOPERATOR(), toXContent);
    return toXContent;
  }

  public static boolean isQueryFunction(String methodName) {
    return queryFunctions.contains(methodName.toLowerCase());
  }

  /**
   * Helper method used to form a range query object for the date_format function.
   *
   * <p>Example: WHERE date_format(dateField, "YYYY-MM-dd") > "2012-01-01" Expected range query:
   * "range": { "dateField": { "from": "2012-01-01", "to": null, "include_lower": false,
   * "include_upper": true, "time_zone": "America/Los_Angeles", "format": "YYYY-MM-dd", "boost": 1 }
   * }
   *
   * @param nameExpr SQL method expression (ex. date_format(dateField, "YYYY-MM-dd"))
   * @param valueExpr Value expression being compared to the SQL method result (ex. "2012-01-01")
   * @throws SqlParseException
   */
  private ToXContent makeForDateFormat(SQLMethodInvokeExpr nameExpr, SQLCharExpr valueExpr)
      throws SqlParseException {
    ToXContent toXContent = null;
    List<SQLExpr> params = nameExpr.getParameters();

    String field = params.get(0).toString();
    String format = removeSingleQuote(params.get(1).toString());
    String dateToCompare = valueExpr.getText();
    String oper = ((SQLBinaryOpExpr) nameExpr.getParent()).getOperator().name;

    String zoneId;
    if (params.size() > 2) {
      zoneId = ZoneId.of(removeSingleQuote(params.get(2).toString())).toString();
    } else {
      // Using UTC, if there is no Zone provided.
      zoneId = UTC.getId();
    }

    RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(field).format(format).timeZone(zoneId);
    switch (oper) {
      case "<>":
      case "=":
        toXContent = rangeQuery.gte(dateToCompare).lte(dateToCompare);
        break;
      case ">":
        toXContent = rangeQuery.gt(dateToCompare);
        break;
      case "<":
        toXContent = rangeQuery.lt(dateToCompare);
        break;
      case ">=":
        toXContent = rangeQuery.gte(dateToCompare);
        break;
      case "<=":
        toXContent = rangeQuery.lte(dateToCompare);
        break;
      case "BETWEEN":
      case "NOT BETWEEN":
        // todo: Add support for BETWEEN
        break;
      default:
        throw new SqlParseException("date_format does not support the operation " + oper);
    }

    toXContent = applyNot(Condition.OPERATOR.operStringToOpear.get(oper), toXContent);
    return toXContent;
  }

  private String removeSingleQuote(String param) {
    return param.replaceAll("\'", "");
  }

  private String[] arrayOfObjectsToStringArray(Object[] values, int from, int to) {
    String[] strings = new String[to - from + 1];
    int counter = 0;
    for (int i = from; i <= to; i++) {
      strings[counter] = values[i].toString();
      counter++;
    }
    return strings;
  }

  private ShapeBuilder getShapeBuilderFromString(String str) throws IOException, SqlParseException {
    String json;
    if (str.contains("{")) {
      json = fixJsonFromElastic(str);
    } else {
      json = WktToGeoJsonConverter.toGeoJson(trimApostrophes(str));
    }

    return getShapeBuilderFromJson(json);
  }

  /*
   * elastic sends {coordinates=[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]],
   * type=Polygon}
   * proper form is {"coordinates":[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]],
   * "type":"Polygon"}
   *  */
  private String fixJsonFromElastic(String elasticJson) {
    String properJson = elasticJson.replaceAll("=", ":");
    properJson = properJson.replaceAll("(type)(:)([a-zA-Z]+)", "\"type\":\"$3\"");
    properJson = properJson.replaceAll("coordinates", "\"coordinates\"");
    return properJson;
  }

  private ShapeBuilder getShapeBuilderFromJson(String json) throws IOException {
    XContentParser parser = null;
    parser =
        JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json);
    parser.nextToken();
    return ShapeParser.parse(parser);
  }

  private String trimApostrophes(String str) {
    return str.substring(1, str.length() - 1);
  }

  /** Applies negation to query builder if the operation is a "not" operation. */
  private ToXContent applyNot(Condition.OPERATOR OPERATOR, ToXContent bqb) {
    if (NOT_OPERATOR_SET.contains(OPERATOR)) {
      bqb = QueryBuilders.boolQuery().mustNot((QueryBuilder) bqb);
    }
    return bqb;
  }

  private Object parseTermValue(Object termValue) {
    if (termValue instanceof SQLNumericLiteralExpr) {
      termValue = ((SQLNumericLiteralExpr) termValue).getNumber();
      if (termValue instanceof BigDecimal || termValue instanceof Double) {
        termValue = ((Number) termValue).doubleValue();
      } else if (termValue instanceof Float) {
        termValue = ((Number) termValue).floatValue();
      } else if (termValue instanceof BigInteger || termValue instanceof Long) {
        termValue = ((Number) termValue).longValue();
      } else if (termValue instanceof Integer) {
        termValue = ((Number) termValue).intValue();
      } else if (termValue instanceof Short) {
        termValue = ((Number) termValue).shortValue();
      } else if (termValue instanceof Byte) {
        termValue = ((Number) termValue).byteValue();
      }
    } else if (termValue instanceof SQLBooleanExpr) {
      termValue = ((SQLBooleanExpr) termValue).getValue();
    } else {
      termValue = termValue.toString();
    }

    return termValue;
  }
}
