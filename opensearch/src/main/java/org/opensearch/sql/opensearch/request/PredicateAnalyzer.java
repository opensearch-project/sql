/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Spark project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.opensearch.request;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.existsQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.regexpQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;
import static org.opensearch.index.query.QueryBuilders.wildcardQuery;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.MULTI_FIELDS_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.SINGLE_FIELD_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.COMPOUNDED_LANG_NAME;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.UnsupportedScriptException;
import org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.ScriptEngineType;
import org.opensearch.sql.opensearch.storage.script.StringUtils;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchBoolPrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhrasePrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MultiMatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryStringQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.SimpleQueryStringQuery;
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer;
import org.opensearch.sql.opensearch.storage.serde.SerializationWrapper;

/**
 * Query predicate analyzer. Uses visitor pattern to traverse existing expression and convert it to
 * {@link QueryBuilder}
 *
 * <p>Major part of this class have been copied from <a
 * href="https://calcite.apache.org/">calcite</a> ES adapter, but it has been changed to support the
 * OpenSearch QueryBuilder.
 *
 * <p>And that file was also sourced from <a href="https://www.dremio.com/">dremio</a> ES adapter
 * (thanks to their team for improving calcite-ES integration).
 */
public class PredicateAnalyzer {

  /** Internal exception. */
  @SuppressWarnings("serial")
  public static final class PredicateAnalyzerException extends RuntimeException {

    PredicateAnalyzerException(String message) {
      super(message);
    }

    PredicateAnalyzerException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Exception that is thrown when a {@link RelNode} expression cannot be processed (or converted
   * into an OpenSearch query).
   */
  public static class ExpressionNotAnalyzableException extends Exception {
    ExpressionNotAnalyzableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private PredicateAnalyzer() {}

  /**
   * Walks the expression tree, attempting to convert the entire tree into an equivalent OpenSearch
   * query filter. If an error occurs, or if it is determined that the expression cannot be
   * converted, an exception is thrown and an error message logged.
   *
   * <p>Callers should catch ExpressionNotAnalyzableException and fall back to not using push-down
   * filters.
   *
   * @param expression expression to analyze
   * @param schema current schema of scan operator
   * @param fieldTypes mapping of OpenSearch field name to ExprType, nested fields are flattened
   * @return search query which can be used to query OS cluster
   * @throws ExpressionNotAnalyzableException when expression can't processed by this analyzer
   */
  public static QueryBuilder analyze(
      RexNode expression, List<String> schema, Map<String, ExprType> fieldTypes)
      throws ExpressionNotAnalyzableException {
    return analyze(expression, schema, fieldTypes, null, null);
  }

  public static QueryBuilder analyze(
      RexNode expression,
      List<String> schema,
      Map<String, ExprType> fieldTypes,
      RelDataType rowType,
      RelOptCluster cluster)
      throws ExpressionNotAnalyzableException {
    return analyzeExpression(expression, schema, fieldTypes, rowType, cluster).builder();
  }

  /**
   * Analyzes the expression and returns a {@link QueryExpression}.
   *
   * @param expression expression to analyze
   * @param schema current schema of scan operator
   * @param fieldTypes mapping of OpenSearch field name to ExprType, nested fields are flattened
   * @return search query which can be used to query OS cluster
   * @throws ExpressionNotAnalyzableException when expression can't processed by this analyzer
   */
  public static QueryExpression analyzeExpression(
      RexNode expression,
      List<String> schema,
      Map<String, ExprType> fieldTypes,
      RelDataType rowType,
      RelOptCluster cluster)
      throws ExpressionNotAnalyzableException {
    requireNonNull(expression, "expression");
    return analyzeExpression(
        expression,
        schema,
        fieldTypes,
        rowType,
        cluster,
        new Visitor(schema, fieldTypes, rowType, cluster));
  }

  /** For test only, passing a customer Visitor */
  public static QueryExpression analyzeExpression(
      RexNode expression,
      List<String> schema,
      Map<String, ExprType> fieldTypes,
      RelDataType rowType,
      RelOptCluster cluster,
      Visitor visitor)
      throws ExpressionNotAnalyzableException {
    requireNonNull(expression, "expression");
    try {
      // visits expression tree
      QueryExpression queryExpression = (QueryExpression) expression.accept(visitor);
      return queryExpression;
    } catch (Throwable e) {
      if (e instanceof UnsupportedScriptException) {
        throw new ExpressionNotAnalyzableException("Can't convert " + expression, e);
      }
      try {
        return new ScriptQueryExpression(expression, rowType, fieldTypes, cluster);
      } catch (Throwable e2) {
        throw new ExpressionNotAnalyzableException("Can't convert " + expression, e2);
      }
    }
  }

  /** Traverses {@link RexNode} tree and builds OpenSearch query. */
  static class Visitor extends RexVisitorImpl<Expression> {

    List<String> schema;
    Map<String, ExprType> fieldTypes;
    RelDataType rowType;
    RelOptCluster cluster;

    Visitor(
        List<String> schema,
        Map<String, ExprType> fieldTypes,
        RelDataType rowType,
        RelOptCluster cluster) {
      super(true);
      this.schema = schema;
      this.fieldTypes = fieldTypes;
      this.rowType = rowType;
      this.cluster = cluster;
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
      return new NamedFieldExpression(inputRef, schema, fieldTypes);
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
      return new LiteralExpression(literal);
    }

    private static boolean supportedRexCall(RexCall call) {
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
        case BINARY:
          switch (call.getKind()) {
            case CONTAINS:
            case AND:
            case OR:
            case LIKE:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
              return true;
            default:
              return false;
          }
        case SPECIAL:
          switch (call.getKind()) {
            case CAST:
            case LIKE:
            case ITEM:
            case OTHER_FUNCTION:
              return true;
            case CASE:
            case SIMILAR:
            default:
              return false;
          }
        case FUNCTION:
          return true;
        case POSTFIX:
          switch (call.getKind()) {
            case IS_TRUE:
            case IS_NOT_NULL:
            case IS_NULL:
              return true;
            default:
              return false;
          }
        case PREFIX: // NOT()
          switch (call.getKind()) {
            case NOT:
              return true;
            default:
              return false;
          }
        case INTERNAL:
          switch (call.getKind()) {
            case SEARCH:
              return true;
            default:
              return false;
          }
        case FUNCTION_ID:
        case FUNCTION_STAR:
        default:
          return false;
      }
    }

    static boolean isSearchWithPoints(RexCall search) {
      RexLiteral literal = (RexLiteral) search.getOperands().get(1);
      final Sarg<?> sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      return sarg.isPoints();
    }

    static boolean isSearchWithComplementedPoints(RexCall search) {
      RexLiteral literal = (RexLiteral) search.getOperands().get(1);
      final Sarg<?> sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      return sarg.isComplementedPoints();
    }

    static RexUnknownAs getNullAsForSearch(RexCall search) {
      RexLiteral literal = (RexLiteral) search.getOperands().get(1);
      final Sarg<?> sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      return sarg.nullAs;
    }

    @Override
    public Expression visitCall(RexCall call) {

      SqlSyntax syntax = call.getOperator().getSyntax();
      if (!supportedRexCall(call)) {
        String message = format(Locale.ROOT, "Unsupported call: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      switch (syntax) {
        case BINARY, INTERNAL:
          return binary(call);
        case POSTFIX:
          return postfix(call);
        case PREFIX:
          return prefix(call);
        case SPECIAL:
          return switch (call.getKind()) {
            case CAST -> toCastExpression(call);
            case CONTAINS -> binary(call);
            case LIKE -> like(call);
            default -> {
              String message = format(Locale.ROOT, "Unsupported call: [%s]", call);
              throw new PredicateAnalyzerException(message);
            }
          };
        case FUNCTION:
          String functionName = call.getOperator().getName().toLowerCase(Locale.ROOT);
          if (functionName.equalsIgnoreCase(UserDefinedFunctionUtils.IP_FUNCTION_NAME)) {
            return visitIpFunction(call);
          } else if (SINGLE_FIELD_RELEVANCE_FUNCTION_SET.contains(functionName)
              || MULTI_FIELDS_RELEVANCE_FUNCTION_SET.contains(functionName)) {
            return visitRelevanceFunc(call);
          }
          // fall through
        default:
          String message =
              format(Locale.ROOT, "Unsupported syntax [%s] for call: [%s]", syntax, call);
          throw new PredicateAnalyzerException(message);
      }
    }

    private QueryExpression visitRelevanceFunc(RexCall call) {
      String funcName = call.getOperator().getName().toLowerCase(Locale.ROOT);
      List<RexNode> ops = call.getOperands();

      // Validate minimum operand count based on function type
      if (SINGLE_FIELD_RELEVANCE_FUNCTION_SET.contains(funcName) && ops.size() < 2) {
        throw new PredicateAnalyzerException(
            "Single field relevance query function should at least have 2 operands (field and"
                + " query)");
      } else if (MULTI_FIELDS_RELEVANCE_FUNCTION_SET.contains(funcName) && ops.size() < 1) {
        throw new PredicateAnalyzerException(
            "Multi field relevance query function should at least have 1 operand (query)");
      }

      if (SINGLE_FIELD_RELEVANCE_FUNCTION_SET.contains(funcName)) {
        List<Expression> fieldQueryOperands =
            visitList(
                List.of(
                    AliasPair.from(ops.get(0), funcName).value,
                    AliasPair.from(ops.get(1), funcName).value));
        NamedFieldExpression namedFieldExpression =
            (NamedFieldExpression) fieldQueryOperands.get(0);
        String queryLiteralOperand = ((LiteralExpression) fieldQueryOperands.get(1)).stringValue();
        Map<String, String> optionalArguments =
            parseRelevanceFunctionOptionalArguments(ops, funcName);

        return SINGLE_FIELD_RELEVANCE_FUNCTION_HANDLERS
            .get(funcName)
            .apply(namedFieldExpression, queryLiteralOperand, optionalArguments);
      } else if (MULTI_FIELDS_RELEVANCE_FUNCTION_SET.contains(funcName)) {
        // Handle both syntaxes:
        // 1. func([fieldExpressions], query, option) - fields are present
        // 2. func(query, optional) - fields are not present
        RexCall fieldsRexCall = null;
        String queryLiteralOperand;
        Map<String, String> optionalArguments;

        // Check if the first argument is fields or query by looking for "fields" key
        AliasPair firstPair = AliasPair.from(ops.get(0), funcName);
        String firstKey = ((RexLiteral) firstPair.alias).getValueAs(String.class);

        if ("fields".equals(firstKey)) {
          // Syntax 1: func([fieldExpressions], query, option)
          fieldsRexCall = (RexCall) firstPair.value;
          queryLiteralOperand =
              ((LiteralExpression)
                      visitList(List.of(AliasPair.from(ops.get(1), funcName).value)).get(0))
                  .stringValue();
          optionalArguments = parseRelevanceFunctionOptionalArguments(ops, funcName, 2);
        } else if ("query".equals(firstKey)) {
          // Syntax 2: func(query, optional) - no fields parameter
          queryLiteralOperand =
              ((LiteralExpression) visitList(List.of(firstPair.value)).get(0)).stringValue();
          optionalArguments = parseRelevanceFunctionOptionalArguments(ops, funcName, 1);
        } else {
          throw new PredicateAnalyzerException(
              format(
                  Locale.ROOT,
                  "Invalid first parameter for function [%s]: expected 'fields' or 'query', got"
                      + " '%s'",
                  funcName,
                  firstKey));
        }

        return MULTI_FIELDS_RELEVANCE_FUNCTION_HANDLERS
            .get(funcName)
            .apply(fieldsRexCall, queryLiteralOperand, optionalArguments);
      }

      throw new PredicateAnalyzerException(
          format(Locale.ROOT, "Unsupported search relevance function: [%s]", funcName));
    }

    private LiteralExpression visitIpFunction(RexCall call) {
      return new LiteralExpression((RexLiteral) call.getOperands().getFirst());
    }

    @FunctionalInterface
    private interface SingleFieldRelevanceFunctionHandler {
      QueryExpression apply(NamedFieldExpression field, String query, Map<String, String> opts);
    }

    @FunctionalInterface
    private interface MultiFieldsRelevanceFunctionHandler {
      QueryExpression apply(RexCall fields, String query, Map<String, String> opts);
    }

    private static final Map<String, SingleFieldRelevanceFunctionHandler>
        SINGLE_FIELD_RELEVANCE_FUNCTION_HANDLERS =
            Map.of(
                "match", (f, q, o) -> QueryExpression.create(f).match(q, o),
                "match_phrase", (f, q, o) -> QueryExpression.create(f).matchPhrase(q, o),
                "match_bool_prefix", (f, q, o) -> QueryExpression.create(f).matchBoolPrefix(q, o),
                "match_phrase_prefix",
                    (f, q, o) -> QueryExpression.create(f).matchPhrasePrefix(q, o));

    private static final Map<String, MultiFieldsRelevanceFunctionHandler>
        MULTI_FIELDS_RELEVANCE_FUNCTION_HANDLERS =
            Map.of(
                "simple_query_string",
                    (c, q, o) ->
                        QueryExpression.create(new NamedFieldExpression())
                            .simpleQueryString(c, q, o),
                "query_string",
                    (c, q, o) ->
                        QueryExpression.create(new NamedFieldExpression()).queryString(c, q, o),
                "multi_match",
                    (c, q, o) ->
                        QueryExpression.create(new NamedFieldExpression()).multiMatch(c, q, o));

    private Map<String, String> parseRelevanceFunctionOptionalArguments(
        List<RexNode> operands, String funcName) {
      return parseRelevanceFunctionOptionalArguments(operands, funcName, 2);
    }

    private Map<String, String> parseRelevanceFunctionOptionalArguments(
        List<RexNode> operands, String funcName, int startIndex) {
      Map<String, String> optionalArguments = new HashMap<>();

      for (int i = startIndex; i < operands.size(); i++) {
        AliasPair aliasPair = AliasPair.from(operands.get(i), funcName);
        String key = ((RexLiteral) aliasPair.alias).getValueAs(String.class);
        if (optionalArguments.containsKey(key)) {
          throw new PredicateAnalyzerException(
              format(
                  Locale.ROOT,
                  "Parameter '%s' can only be specified once for function [%s].",
                  key,
                  funcName));
        }
        optionalArguments.put(key, ((RexLiteral) aliasPair.value).getValueAs(String.class));
      }

      return optionalArguments;
    }

    private static RexCall expectCall(RexNode node, SqlOperator op, String funcName) {
      if (!(node instanceof RexCall call) || call.getOperator() != op) {
        throw new IllegalArgumentException(
            format(
                Locale.ROOT,
                "Expect [%s] RexCall but get [%s] for function [%s]",
                op.getName(),
                node.toString(),
                funcName));
      }
      return call;
    }

    private static class AliasPair {
      final RexNode value;
      final RexNode alias;

      static AliasPair from(RexNode node, String funcName) {
        RexCall mapCall = expectCall(node, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, funcName);
        return new AliasPair(mapCall.getOperands().get(1), mapCall.getOperands().get(0));
      }

      private AliasPair(RexNode value, RexNode alias) {
        this.value = value;
        this.alias = alias;
      }
    }

    private QueryExpression prefix(RexCall call) {
      checkArgument(
          call.getKind() == SqlKind.NOT, "Expected %s got %s", SqlKind.NOT, call.getKind());

      if (call.getOperands().size() != 1) {
        String message = format(Locale.ROOT, "Unsupported NOT operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      QueryExpression expr = (QueryExpression) call.getOperands().get(0).accept(this);
      return expr.not();
    }

    private QueryExpression postfix(RexCall call) {
      checkArgument(
          call.getKind() == SqlKind.IS_TRUE
              || call.getKind() == SqlKind.IS_NULL
              || call.getKind() == SqlKind.IS_NOT_NULL);
      if (call.getOperands().size() != 1) {
        String message = format(Locale.ROOT, "Unsupported operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      if (call.getKind() == SqlKind.IS_TRUE) {
        Expression qe = call.getOperands().get(0).accept(this);
        return ((QueryExpression) qe).isTrue();
      }

      // OpenSearch DSL does not handle IS_NULL / IS_NOT_NULL on nested fields correctly
      checkForNestedFieldOperands(call);

      Expression a = call.getOperands().get(0).accept(this);
      // OpenSearch does not want is null/is not null (exists query)
      // for _id and _index, although it supports for all other metadata column
      isColumn(a, call, OpenSearchConstants.METADATA_FIELD_ID, true);
      isColumn(a, call, OpenSearchConstants.METADATA_FIELD_INDEX, true);
      QueryExpression operand = QueryExpression.create((TerminalExpression) a);
      return call.getKind() == SqlKind.IS_NOT_NULL ? operand.exists() : operand.notExists();
    }

    /**
     * Process a call which is a binary operation, transforming into an equivalent query expression.
     * Note that the incoming call may be either a simple binary expression, such as {@code foo >
     * 5}, or it may be several simple expressions connected by {@code AND} or {@code OR} operators,
     * such as {@code foo > 5 AND bar = 'abc' AND 'rot' < 1}
     *
     * @param call existing call
     * @return evaluated expression
     */
    private QueryExpression binary(RexCall call) {

      // if AND/OR, do special handling
      if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
        return andOr(call);
      }

      checkForIncompatibleDateTimeOperands(call);

      checkState(call.getOperands().size() == 2);
      final Expression a = call.getOperands().get(0).accept(this);
      final Expression b = call.getOperands().get(1).accept(this);

      final SwapResult pair = swap(a, b);
      final boolean swapped = pair.isSwapped();

      // For _id and _index columns, only equals/not_equals work!
      if (isColumn(pair.getKey(), call, OpenSearchConstants.METADATA_FIELD_ID, false)
          || isColumn(pair.getKey(), call, OpenSearchConstants.METADATA_FIELD_INDEX, false)
          || isColumn(pair.getKey(), call, OpenSearchConstants.METADATA_FIELD_UID, false)) {
        switch (call.getKind()) {
          case EQUALS:
          case NOT_EQUALS:
            break;
          default:
            throw new PredicateAnalyzerException(
                "Cannot handle " + call.getKind() + " expression for _id field, " + call);
        }
      }

      switch (call.getKind()) {
        case CONTAINS:
          return QueryExpression.create(pair.getKey()).contains(pair.getValue());
        case EQUALS:
          return QueryExpression.create(pair.getKey()).equals(pair.getValue());
        case NOT_EQUALS:
          return QueryExpression.create(pair.getKey()).notEquals(pair.getValue());
        case GREATER_THAN:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).lt(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).gt(pair.getValue());
        case GREATER_THAN_OR_EQUAL:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).lte(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).gte(pair.getValue());
        case LESS_THAN:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).gt(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).lt(pair.getValue());
        case LESS_THAN_OR_EQUAL:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).gte(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).lte(pair.getValue());
        case SEARCH:
          QueryExpression expression = constructQueryExpressionForSearch(call, pair);
          RexUnknownAs nullAs = getNullAsForSearch(call);
          QueryExpression finalExpression =
              switch (nullAs) {
                  // e.g. where isNotNull(a) and (a = 1 or a = 2)
                  // TODO: For this case, seems return `expression` should be equivalent
                case FALSE -> CompoundQueryExpression.and(
                    false, expression, QueryExpression.create(pair.getKey()).exists());
                  // e.g. where isNull(a) or a = 1 or a = 2
                case TRUE -> CompoundQueryExpression.or(
                    expression, QueryExpression.create(pair.getKey()).notExists());
                  // e.g. where a = 1 or a = 2
                case UNKNOWN -> expression;
              };
          finalExpression.updateAnalyzedNodes(call);
          return finalExpression;
        default:
          break;
      }
      String message = format(Locale.ROOT, "Unable to handle call: [%s]", call);
      throw new PredicateAnalyzerException(message);
    }

    private QueryExpression like(RexCall call) {
      // The third default escape is not used here. It's handled by
      // StringUtils.convertSqlWildcardToLucene
      checkState(call.getOperands().size() == 3);
      final Expression a = call.getOperands().get(0).accept(this);
      final Expression b = call.getOperands().get(1).accept(this);
      final SwapResult pair = swap(a, b);
      return QueryExpression.create(pair.getKey()).like(pair.getValue());
    }

    private static QueryExpression constructQueryExpressionForSearch(
        RexCall call, SwapResult pair) {
      if (isSearchWithComplementedPoints(call)) {
        return QueryExpression.create(pair.getKey()).notIn(pair.getValue());
      } else if (isSearchWithPoints(call)) {
        return QueryExpression.create(pair.getKey()).in(pair.getValue());
      } else {
        Sarg<?> sarg = pair.getValue().literal.getValueAs(Sarg.class);
        Set<? extends Range<?>> rangeSet = requireNonNull(sarg).rangeSet.asRanges();
        boolean isTimeStamp =
            (pair.getKey() instanceof NamedFieldExpression namedField)
                && namedField.isTimeStampType();
        List<QueryExpression> queryExpressions =
            rangeSet.stream()
                .map(
                    range ->
                        RangeSets.isPoint(range)
                            ? QueryExpression.create(pair.getKey())
                                .equals(sargPointValue(range.lowerEndpoint()), isTimeStamp)
                            : QueryExpression.create(pair.getKey()).between(range, isTimeStamp))
                .toList();
        if (queryExpressions.size() == 1) {
          return queryExpressions.getFirst();
        } else {
          return CompoundQueryExpression.or(queryExpressions.toArray(new QueryExpression[0]));
        }
      }
    }

    private boolean containIsEmptyFunction(RexCall call) {
      return call.getKind() == SqlKind.OR
          && call.getOperands().stream().anyMatch(o -> o.getKind() == SqlKind.IS_NULL)
          && call.getOperands().stream()
              .anyMatch(
                  o ->
                      o.getKind() == SqlKind.OTHER
                          && ((RexCall) o).getOperator().equals(SqlStdOperatorTable.IS_EMPTY));
    }

    private QueryExpression andOr(RexCall call) {
      // For function isEmpty and isBlank, we implement them via expression `isNull or {@function}`,
      // Unlike `OR` in Java, `SHOULD` in DSL will evaluate both branches and lead to NPE.
      if (containIsEmptyFunction(call)) {
        throw new PredicateAnalyzerException(
            "DSL will evaluate both branches of OR with isNUll, prevent push-down to avoid NPE");
      }

      QueryExpression[] expressions = new QueryExpression[call.getOperands().size()];
      PredicateAnalyzerException firstError = null;
      boolean partial = false;
      int failedCount = 0;
      for (int i = 0; i < call.getOperands().size(); i++) {
        RexNode operand = call.getOperands().get(i);
        try {
          Expression expr = tryAnalyzeOperand(operand);
          if (expr instanceof QueryExpression) {
            expressions[i] = (QueryExpression) expr;
            partial |= expressions[i].isPartial();
          }
        } catch (PredicateAnalyzerException e) {
          if (firstError == null) {
            firstError = e;
          }
          partial = true;
          ++failedCount;
          // If we cannot analyze the operand, wrap the RexNode with UnAnalyzableQueryExpression and
          // record them in the array. We will reuse them later.
          expressions[i] = new UnAnalyzableQueryExpression(operand);
        }
      }

      switch (call.getKind()) {
        case OR:
          if (partial) {
            if (firstError != null) {
              throw firstError;
            } else {
              final String message = format(Locale.ROOT, "Unable to handle call: [%s]", call);
              throw new PredicateAnalyzerException(message);
            }
          }
          return CompoundQueryExpression.or(expressions);
        case AND:
          if (failedCount == call.getOperands().size()) {
            // If all operands failed, we cannot analyze the AND expression.
            throw new PredicateAnalyzerException(
                "All expressions in AND failed to analyze: " + call);
          }
          return CompoundQueryExpression.and(partial, expressions);
        default:
          String message = format(Locale.ROOT, "Unable to handle call: [%s]", call);
          throw new PredicateAnalyzerException(message);
      }
    }

    public Expression tryAnalyzeOperand(RexNode node) {
      try {
        Expression expr = node.accept(this);
        if (expr instanceof NamedFieldExpression) {
          return expr;
        }
        QueryExpression qe = (QueryExpression) expr;
        if (!qe.isPartial()) {
          qe.updateAnalyzedNodes(node);
        }
        return qe;
      } catch (PredicateAnalyzerException firstFailed) {
        try {
          QueryExpression qe = new ScriptQueryExpression(node, rowType, fieldTypes, cluster);
          if (!qe.isPartial()) {
            qe.updateAnalyzedNodes(node);
          }
          return qe;
        } catch (UnsupportedScriptException secondFailed) {
          throw new PredicateAnalyzerException(secondFailed);
        }
      }
    }

    /**
     * Holder class for a pair of expressions. Used to convert {@code 1 = foo} into {@code foo = 1}
     */
    private static class SwapResult {
      final boolean swapped;
      final TerminalExpression terminal;
      final LiteralExpression literal;

      SwapResult(boolean swapped, TerminalExpression terminal, LiteralExpression literal) {
        super();
        this.swapped = swapped;
        this.terminal = terminal;
        this.literal = literal;
      }

      TerminalExpression getKey() {
        return terminal;
      }

      LiteralExpression getValue() {
        return literal;
      }

      boolean isSwapped() {
        return swapped;
      }
    }

    /**
     * Swap order of operands such that the literal expression is always on the right.
     *
     * <p>NOTE: Some combinations of operands are implicitly not supported and will cause an
     * exception to be thrown. For example, we currently do not support comparing a literal to
     * another literal as convention {@code 5 = 5}. Nor do we support comparing named fields to
     * other named fields as convention {@code $0 = $1}.
     *
     * @param left left expression
     * @param right right expression
     */
    private static SwapResult swap(Expression left, Expression right) {

      TerminalExpression terminal;
      LiteralExpression literal = expressAsLiteral(left);
      boolean swapped = false;
      if (literal != null) {
        swapped = true;
        terminal = (TerminalExpression) right;
      } else {
        literal = expressAsLiteral(right);
        terminal = (TerminalExpression) left;
      }

      if (literal == null || terminal == null) {
        String message =
            format(
                Locale.ROOT,
                "Unexpected combination of expressions [left: %s] [right: %s]",
                left,
                right);
        throw new PredicateAnalyzerException(message);
      }

      if (CastExpression.isCastExpression(terminal)) {
        terminal = CastExpression.unpack(terminal);
      }

      return new SwapResult(swapped, terminal, literal);
    }

    private CastExpression toCastExpression(RexCall call) {
      TerminalExpression argument = (TerminalExpression) call.getOperands().get(0).accept(this);
      return new CastExpression(call.getType(), argument);
    }

    private static NamedFieldExpression toNamedField(RexLiteral literal) {
      return new NamedFieldExpression(literal);
    }

    /** Try to convert a generic expression into a literal expression. */
    private static LiteralExpression expressAsLiteral(Expression exp) {

      if (exp instanceof LiteralExpression) {
        return (LiteralExpression) exp;
      }

      return null;
    }

    private static boolean isColumn(
        Expression exp, RexNode node, String columnName, boolean throwException) {
      if (!(exp instanceof NamedFieldExpression)) {
        return false;
      }

      final NamedFieldExpression termExp = (NamedFieldExpression) exp;
      if (columnName.equals(termExp.getRootName())) {
        if (throwException) {
          throw new PredicateAnalyzerException("Cannot handle _id field in " + node);
        }
        return true;
      }
      return false;
    }
  }

  /** Empty interface; exists only to define the type hierarchy. */
  interface Expression {}

  /** Main expression operators (like {@code equals}, {@code gt}, {@code exists} etc.) */
  @Getter
  public abstract static class QueryExpression implements Expression {
    private int scriptCount;

    protected void accumulateScriptCount(int count) {
      scriptCount += count;
    }

    public abstract QueryBuilder builder();

    public abstract List<RexNode> getAnalyzedNodes();

    public abstract void updateAnalyzedNodes(RexNode rexNode);

    public abstract List<RexNode> getUnAnalyzableNodes();

    public boolean isPartial() {
      return false;
    }

    /** Negate {@code this} QueryExpression (not the next one). */
    QueryExpression not() {
      throw new PredicateAnalyzerException("not cannot be applied to " + this.getClass());
    }

    QueryExpression exists() {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['exists'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression notExists() {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['notExists'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression contains(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['contains'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression between(Range<?> literal, boolean isTimeStamp) {
      throw new PredicateAnalyzerException("between cannot be applied to " + this.getClass());
    }

    QueryExpression like(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['like'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression notLike(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['notLike'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression equals(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['='] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression equals(Object point, boolean isTimeStamp) {
      throw new PredicateAnalyzerException("equals cannot be applied to " + this.getClass());
    }

    QueryExpression notEquals(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['not'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression gt(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['>'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression gte(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['>='] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression lt(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['<'] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression lte(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['<='] " + "cannot be applied to " + this.getClass());
    }

    QueryExpression match(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException("Match " + "cannot be applied to " + this.getClass());
    }

    QueryExpression matchPhrase(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchPhrase " + "cannot be applied to " + this.getClass());
    }

    QueryExpression matchBoolPrefix(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchBoolPrefix " + "cannot be applied to " + this.getClass());
    }

    QueryExpression matchPhrasePrefix(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchPhrasePrefix " + "cannot be applied to " + this.getClass());
    }

    QueryExpression simpleQueryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "SimpleQueryString " + "cannot be applied to " + this.getClass());
    }

    QueryExpression queryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "QueryString " + "cannot be applied to " + this.getClass());
    }

    QueryExpression multiMatch(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MultiMatch " + "cannot be applied to " + this.getClass());
    }

    QueryExpression isTrue() {
      throw new PredicateAnalyzerException("isTrue cannot be applied to " + this.getClass());
    }

    QueryExpression in(LiteralExpression literal) {
      throw new PredicateAnalyzerException("in cannot be applied to " + this.getClass());
    }

    QueryExpression notIn(LiteralExpression literal) {
      throw new PredicateAnalyzerException("notIn cannot be applied to " + this.getClass());
    }

    static QueryExpression create(TerminalExpression expression) {
      if (expression instanceof CastExpression) {
        expression = CastExpression.unpack(expression);
      }

      if (expression instanceof NamedFieldExpression) {
        return new SimpleQueryExpression((NamedFieldExpression) expression);
      } else {
        String message = format(Locale.ROOT, "Unsupported expression: [%s]", expression);
        throw new PredicateAnalyzerException(message);
      }
    }
  }

  @Getter
  static class UnAnalyzableQueryExpression extends QueryExpression {
    final RexNode unAnalyzableRexNode;

    public UnAnalyzableQueryExpression(RexNode rexNode) {
      this.unAnalyzableRexNode = requireNonNull(rexNode, "rexNode");
    }

    @Override
    public QueryBuilder builder() {
      return null;
    }

    @Override
    public List<RexNode> getUnAnalyzableNodes() {
      return List.of(unAnalyzableRexNode);
    }

    @Override
    public List<RexNode> getAnalyzedNodes() {
      return List.of();
    }

    @Override
    public void updateAnalyzedNodes(RexNode rexNode) {
      throw new IllegalStateException(
          "UnAnalyzableQueryExpression does not support unAnalyzableNodes");
    }
  }

  /** Builds conjunctions / disjunctions based on existing expressions. */
  public static class CompoundQueryExpression extends QueryExpression {

    private final boolean partial;
    private final BoolQueryBuilder builder;
    @Getter private List<RexNode> analyzedNodes = new ArrayList<>();
    @Getter private final List<RexNode> unAnalyzableNodes = new ArrayList<>();

    public static CompoundQueryExpression or(QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(false);
      for (QueryExpression expression : expressions) {
        bqe.builder.should(expression.builder());
        bqe.accumulateScriptCount(expression.getScriptCount());
      }
      return bqe;
    }

    /**
     * If partial expression, we will need to complete it with a full filter.
     *
     * @param partial whether we partially converted a and for push down purposes
     * @param expressions list of expressions to join with {@code and} boolean
     * @return new instance of expression
     */
    public static CompoundQueryExpression and(boolean partial, QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(partial);
      for (QueryExpression expression : expressions) {
        bqe.analyzedNodes.addAll(expression.getAnalyzedNodes());
        bqe.unAnalyzableNodes.addAll(expression.getUnAnalyzableNodes());
        if (!(expression instanceof UnAnalyzableQueryExpression)) {
          bqe.builder.must(expression.builder());
          bqe.accumulateScriptCount(expression.getScriptCount());
        }
      }
      return bqe;
    }

    private CompoundQueryExpression(boolean partial) {
      this(partial, boolQuery());
    }

    private CompoundQueryExpression(boolean partial, BoolQueryBuilder builder) {
      this.partial = partial;
      this.builder = requireNonNull(builder, "builder");
    }

    @Override
    public boolean isPartial() {
      return partial;
    }

    @Override
    public QueryBuilder builder() {
      return builder;
    }

    @Override
    public void updateAnalyzedNodes(RexNode rexNode) {
      this.analyzedNodes = List.of(rexNode);
    }

    @Override
    public QueryExpression not() {
      return new CompoundQueryExpression(partial, boolQuery().mustNot(builder()));
    }
  }

  /** Usually basic expression of type {@code a = 'val'} or {@code b > 42}. */
  static class SimpleQueryExpression extends QueryExpression {

    private RexNode analyzedRexNode;
    private final NamedFieldExpression rel;
    private QueryBuilder builder;

    private String getFieldReference() {
      return rel.getReference();
    }

    private String getFieldReferenceForTermQuery() {
      String reference = rel.getReferenceForTermQuery();
      // Throw exception in advance of method builder() to trigger partial push down.
      if (reference == null) {
        throw new PredicateAnalyzerException(
            "Field reference for term query cannot be null for " + rel.getRootName());
      }
      return reference;
    }

    private SimpleQueryExpression(NamedFieldExpression rel) {
      this.rel = rel;
    }

    public SimpleQueryExpression(QueryBuilder builder) {
      this.builder = builder;
      this.rel = null;
    }

    @Override
    public QueryBuilder builder() {
      if (builder == null) {
        throw new IllegalStateException("Builder was not initialized");
      }
      return builder;
    }

    @Override
    public List<RexNode> getUnAnalyzableNodes() {
      return List.of();
    }

    @Override
    public List<RexNode> getAnalyzedNodes() {
      return analyzedRexNode == null ? List.of() : List.of(analyzedRexNode);
    }

    @Override
    public void updateAnalyzedNodes(RexNode rexNode) {
      this.analyzedRexNode = rexNode;
    }

    @Override
    public QueryExpression not() {
      builder = boolQuery().mustNot(builder());
      return this;
    }

    @Override
    public QueryExpression exists() {
      builder = existsQuery(getFieldReference());
      return this;
    }

    @Override
    public QueryExpression notExists() {
      // Even though Lucene doesn't allow a stand alone mustNot boolean query,
      // OpenSearch handles this problem transparently on its end
      builder = boolQuery().mustNot(existsQuery(getFieldReference()));
      return this;
    }

    /*
     * Prefer to run wildcard query for keyword type field. For text type field, it doesn't support
     * cross term match because OpenSearch internally break text to multiple terms and apply wildcard
     * matching one by one, which is not same behavior with regular like function without pushdown.
     */
    @Override
    public QueryExpression like(LiteralExpression literal) {
      String fieldName = getFieldReference();
      String keywordField = OpenSearchTextType.toKeywordSubField(fieldName, this.rel.getExprType());
      boolean isKeywordField = keywordField != null;
      if (isKeywordField) {
        builder =
            wildcardQuery(
                    keywordField, StringUtils.convertSqlWildcardToLuceneSafe(literal.stringValue()))
                .caseInsensitive(true);
        return this;
      }
      throw new UnsupportedOperationException("Like query is not supported for text field");
    }

    @Override
    public QueryExpression contains(LiteralExpression literal) {
      builder = matchQuery(getFieldReference(), literal.value());
      return this;
    }

    @Override
    public QueryExpression notLike(LiteralExpression literal) {
      builder =
          boolQuery()
              // NOT LIKE should return false when field is NULL
              .must(existsQuery(getFieldReference()))
              .mustNot(regexpQuery(getFieldReference(), literal.stringValue()));
      return this;
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder =
            boolQuery()
                .must(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gte(value)))
                .must(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lte(value)));
      } else {
        builder = termQuery(getFieldReferenceForTermQuery(), value);
      }
      return this;
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder =
            boolQuery()
                .should(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gt(value)))
                .should(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lt(value)));
      } else {
        builder =
            boolQuery()
                // NOT LIKE should return false when field is NULL
                .must(existsQuery(getFieldReference()))
                .mustNot(termQuery(getFieldReferenceForTermQuery(), value));
      }
      return this;
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gt(value));
      return this;
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gte(value));
      return this;
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lt(value));
      return this;
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lte(value));
      return this;
    }

    @Override
    public QueryExpression match(String query, Map<String, String> optionalArguments) {
      builder = new MatchQuery().build(getFieldReference(), query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression matchPhrase(String query, Map<String, String> optionalArguments) {
      builder = new MatchPhraseQuery().build(getFieldReference(), query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression matchBoolPrefix(String query, Map<String, String> optionalArguments) {
      builder = new MatchBoolPrefixQuery().build(getFieldReference(), query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression matchPhrasePrefix(String query, Map<String, String> optionalArguments) {
      builder = new MatchPhrasePrefixQuery().build(getFieldReference(), query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression simpleQueryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      builder = new SimpleQueryStringQuery().build(fieldsRexCall, query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression queryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      builder = new QueryStringQuery().build(fieldsRexCall, query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression multiMatch(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      builder = new MultiMatchQuery().build(fieldsRexCall, query, optionalArguments);
      return this;
    }

    @Override
    public QueryExpression isTrue() {
      // Ignore istrue if ISTRUE(predicate) and will support ISTRUE(field) later.
      // builder = termQuery(getFieldReferenceForTermQuery(), true);
      return this;
    }

    @Override
    public QueryExpression in(LiteralExpression literal) {
      Collection<?> collection = (Collection<?>) literal.value();
      builder = termsQuery(getFieldReferenceForTermQuery(), collection);
      return this;
    }

    @Override
    public QueryExpression notIn(LiteralExpression literal) {
      Collection<?> collection = (Collection<?>) literal.value();
      builder = boolQuery().mustNot(termsQuery(getFieldReferenceForTermQuery(), collection));
      return this;
    }

    @Override
    public QueryExpression equals(Object point, boolean isTimeStamp) {
      builder =
          termQuery(getFieldReferenceForTermQuery(), convertEndpointValue(point, isTimeStamp));
      return this;
    }

    @Override
    public QueryExpression between(Range<?> range, boolean isTimeStamp) {
      Object lowerBound =
          range.hasLowerBound() ? convertEndpointValue(range.lowerEndpoint(), isTimeStamp) : null;
      Object upperBound =
          range.hasUpperBound() ? convertEndpointValue(range.upperEndpoint(), isTimeStamp) : null;
      RangeQueryBuilder rangeQueryBuilder = rangeQuery(getFieldReference());
      rangeQueryBuilder =
          range.lowerBoundType() == BoundType.CLOSED
              ? rangeQueryBuilder.gte(lowerBound)
              : rangeQueryBuilder.gt(lowerBound);
      rangeQueryBuilder =
          range.upperBoundType() == BoundType.CLOSED
              ? rangeQueryBuilder.lte(upperBound)
              : rangeQueryBuilder.lt(upperBound);
      builder = rangeQueryBuilder;
      return this;
    }

    private Object convertEndpointValue(Object value, boolean isTimeStamp) {
      value = sargPointValue(value);
      return isTimeStamp ? timestampValueForPushDown(value.toString()) : value;
    }
  }

  private static String timestampValueForPushDown(String value) {
    ExprTimestampValue exprTimestampValue = new ExprTimestampValue(value);
    return DateFieldMapper.getDefaultDateTimeFormatter()
        .format(exprTimestampValue.timestampValue());
    // https://github.com/opensearch-project/sql/pull/3442
  }

  private static String ipValueForPushDown(String value) {
    ExprIpValue exprIpValue = new ExprIpValue(value);
    return exprIpValue.value();
  }

  public static class ScriptQueryExpression extends QueryExpression {
    private RexNode analyzedNode;
    // use lambda to generate code lazily to avoid store generated code
    private final Supplier<String> codeGenerator;

    public ScriptQueryExpression(
        RexNode rexNode,
        RelDataType rowType,
        Map<String, ExprType> fieldTypes,
        RelOptCluster cluster) {
      // We prevent is_null(nested_field) from being pushed down because pushed-down scripts can not
      // access nested fields for the time being
      if (rexNode instanceof RexCall
          && (rexNode.getKind().equals(SqlKind.IS_NULL)
              || rexNode.getKind().equals(SqlKind.IS_NOT_NULL))) {
        checkForNestedFieldOperands((RexCall) rexNode);
      }
      accumulateScriptCount(1);
      RelJsonSerializer serializer = new RelJsonSerializer(cluster);
      this.codeGenerator =
          () ->
              SerializationWrapper.wrapWithLangType(
                  ScriptEngineType.CALCITE, serializer.serialize(rexNode, rowType, fieldTypes));
    }

    @Override
    public QueryBuilder builder() {
      return new ScriptQueryBuilder(getScript());
    }

    public Script getScript() {
      long currentTime = Hook.CURRENT_TIME.get(-1L);
      if (currentTime < 0) {
        throw new UnsupportedScriptException(
            "ScriptQueryExpression requires a valid current time from hook, but it is not set");
      }
      return new Script(
          DEFAULT_SCRIPT_TYPE,
          COMPOUNDED_LANG_NAME,
          codeGenerator.get(),
          Collections.emptyMap(),
          Map.of(Variable.UTC_TIMESTAMP.camelName, currentTime));
    }

    @Override
    public List<RexNode> getAnalyzedNodes() {
      return analyzedNode == null ? List.of() : List.of(analyzedNode);
    }

    @Override
    public void updateAnalyzedNodes(RexNode rexNode) {
      this.analyzedNode = rexNode;
    }

    @Override
    public List<RexNode> getUnAnalyzableNodes() {
      return List.of();
    }
  }

  /**
   * By default, range queries on date/time need use the format of the source to parse the literal.
   * So we need to specify that the literal has "date_time" format
   *
   * @param literal literal value
   * @param rangeQueryBuilder query builder to optionally add {@code format} expression
   * @return existing builder with possible {@code format} attribute
   */
  private static RangeQueryBuilder addFormatIfNecessary(
      LiteralExpression literal, RangeQueryBuilder rangeQueryBuilder) {
    if (literal.value() instanceof GregorianCalendar) {
      rangeQueryBuilder.format("date_time");
    }
    return rangeQueryBuilder;
  }

  /** Empty interface; exists only to define the type hierarchy. */
  interface TerminalExpression extends Expression {}

  /** SQL cast. For example, {@code cast(col as INTEGER)}. */
  static final class CastExpression implements TerminalExpression {
    @SuppressWarnings("unused")
    private final RelDataType type;

    private final TerminalExpression argument;

    private CastExpression(RelDataType type, TerminalExpression argument) {
      this.type = type;
      this.argument = argument;
    }

    public boolean isCastFromLiteral() {
      return argument instanceof LiteralExpression;
    }

    static TerminalExpression unpack(TerminalExpression exp) {
      if (!(exp instanceof CastExpression)) {
        return exp;
      }
      return ((CastExpression) exp).argument;
    }

    static boolean isCastExpression(Expression exp) {
      return exp instanceof CastExpression;
    }
  }

  /** Used for bind variables. */
  public static final class NamedFieldExpression implements TerminalExpression {

    private final String name;
    private final ExprType type;

    public NamedFieldExpression(
        int refIndex, List<String> schema, Map<String, ExprType> filedTypes) {
      this.name = refIndex >= schema.size() ? null : schema.get(refIndex);
      this.type = filedTypes.get(name);
    }

    public NamedFieldExpression(String name, ExprType type) {
      this.name = name;
      this.type = type;
    }

    private NamedFieldExpression() {
      this.name = null;
      this.type = null;
    }

    private NamedFieldExpression(
        RexInputRef ref, List<String> schema, Map<String, ExprType> filedTypes) {
      this.name =
          (ref == null || ref.getIndex() >= schema.size()) ? null : schema.get(ref.getIndex());
      this.type = filedTypes.get(name);
    }

    private NamedFieldExpression(RexLiteral literal) {
      this.name = literal == null ? null : RexLiteral.stringValue(literal);
      this.type = null;
    }

    public String getRootName() {
      return name;
    }

    ExprType getExprType() {
      return type;
    }

    boolean isTimeStampType() {
      return type != null
          && ExprCoreType.TIMESTAMP.equals(
              type.getOriginalExprType() instanceof OpenSearchDataType osType
                  ? osType.getExprCoreType()
                  : type.getOriginalExprType());
    }

    boolean isTextType() {
      return type != null && type.getOriginalExprType() instanceof OpenSearchTextType;
    }

    boolean isMetaField() {
      return OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(getRootName());
    }

    String getReference() {
      return getRootName();
    }

    public String getReferenceForTermQuery() {
      return OpenSearchTextType.toKeywordSubField(getRootName(), this.type);
    }
  }

  /** Literal like {@code 'foo' or 42 or true} etc. */
  static final class LiteralExpression implements TerminalExpression {

    final RexLiteral literal;

    LiteralExpression(RexLiteral literal) {
      this.literal = literal;
    }

    Object value() {

      if (isSarg()) {
        return sargValue();
      } else if (isIntegral()) {
        return longValue();
      } else if (isFractional()) {
        return doubleValue();
      } else if (isBoolean()) {
        return booleanValue();
      } else if (isTimestamp()) {
        return timestampValueForPushDown(RexLiteral.stringValue(literal));
      } else if (isString()) {
        return RexLiteral.stringValue(literal);
      } else if (isIp()) {
        return ipValueForPushDown(RexLiteral.stringValue(literal));
      } else {
        return rawValue();
      }
    }

    boolean isIntegral() {
      return SqlTypeName.INT_TYPES.contains(literal.getType().getSqlTypeName());
    }

    boolean isFractional() {
      return SqlTypeName.FRACTIONAL_TYPES.contains(literal.getType().getSqlTypeName());
    }

    boolean isDecimal() {
      return SqlTypeName.DECIMAL == literal.getType().getSqlTypeName();
    }

    boolean isBoolean() {
      return SqlTypeName.BOOLEAN_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isString() {
      return SqlTypeName.CHAR_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isSarg() {
      return SqlTypeName.SARG.getName().equalsIgnoreCase(literal.getTypeName().getName());
    }

    public boolean isTimestamp() {
      if (literal.getType() instanceof ExprSqlType exprSqlType) {
        return exprSqlType.getUdt() == ExprUDT.EXPR_TIMESTAMP;
      }
      return false;
    }

    public boolean isIp() {
      return literal.getType() instanceof ExprIPType;
    }

    long longValue() {
      return ((Number) literal.getValue()).longValue();
    }

    double doubleValue() {
      return ((Number) literal.getValue()).doubleValue();
    }

    boolean booleanValue() {
      return RexLiteral.booleanValue(literal);
    }

    String stringValue() {
      return RexLiteral.stringValue(literal);
    }

    List<Object> sargValue() {
      final Sarg sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      List<Object> values = new ArrayList<>();
      if (sarg.isPoints()) {
        Set<Range> ranges = sarg.rangeSet.asRanges();
        ranges.forEach(range -> values.add(sargPointValue(range.lowerEndpoint())));
      } else if (sarg.isComplementedPoints()) {
        Set<Range> ranges = sarg.negate().rangeSet.asRanges();
        ranges.forEach(range -> values.add(sargPointValue(range.lowerEndpoint())));
      }
      return values;
    }

    Object rawValue() {
      return literal.getValue();
    }
  }

  /**
   * If the sarg point is a NlsString, we should get the value from it. For BigDecimal type, we
   * should get the double value from the literal. That's because there is no decimal type in
   * OpenSearch.
   */
  public static Object sargPointValue(Object point) {
    if (point instanceof NlsString) {
      return ((NlsString) point).getValue();
    } else if (point instanceof BigDecimal) {
      return ((BigDecimal) point).doubleValue();
    } else {
      return point;
    }
  }

  /**
   * If one operand in a binary operator is a DateTime type, but the other isn't, we should not push
   * down the predicate.
   *
   * @param call Current node being evaluated
   */
  private static void checkForIncompatibleDateTimeOperands(RexCall call) {
    RelDataType op1 = call.getOperands().get(0).getType();
    RelDataType op2 = call.getOperands().get(1).getType();
    if ((SqlTypeFamily.DATETIME.contains(op1) && !SqlTypeFamily.DATETIME.contains(op2))
        || (SqlTypeFamily.DATETIME.contains(op2) && !SqlTypeFamily.DATETIME.contains(op1))
        || (SqlTypeFamily.DATE.contains(op1) && !SqlTypeFamily.DATE.contains(op2))
        || (SqlTypeFamily.DATE.contains(op2) && !SqlTypeFamily.DATE.contains(op1))
        || (SqlTypeFamily.TIMESTAMP.contains(op1) && !SqlTypeFamily.TIMESTAMP.contains(op2))
        || (SqlTypeFamily.TIMESTAMP.contains(op2) && !SqlTypeFamily.TIMESTAMP.contains(op1))
        || (SqlTypeFamily.TIME.contains(op1) && !SqlTypeFamily.TIME.contains(op2))
        || (SqlTypeFamily.TIME.contains(op2) && !SqlTypeFamily.TIME.contains(op1))) {
      throw new PredicateAnalyzerException(
          "Cannot handle " + call.getKind() + " expression for _id field, " + call);
    }
  }

  /**
   * Examines the operands of a RexCall to check for nested fields and throws an exception if any
   * are found.
   *
   * <p>This check prevents operations (IS_NULL, IS_NOT_NULL) that would produce incorrect results
   * in OpenSearch when pushed down as either DSL queries or scripts.
   *
   * @param call The RexCall to check for nested field operands
   * @throws PredicateAnalyzerException if any nested fields are detected in the operands
   */
  private static void checkForNestedFieldOperands(RexCall call) throws PredicateAnalyzerException {
    boolean conditionContainsNestedField =
        call.getOperands().stream()
            .map(RexNode::getType)
            // Nested fields are of type ArraySqlType
            .anyMatch(type -> type instanceof ArraySqlType);
    if (conditionContainsNestedField) {
      throw new PredicateAnalyzerException(
          format(
              Locale.ROOT,
              "OpenSearch DSL does not handle %s on nested fields correctly",
              call.getKind()));
    }
  }
}
