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
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.MULTI_FIELDS_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.SINGLE_FIELD_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.CALCITE_ENGINE_TYPE;
import static org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.COMPOUNDED_LANG_NAME;
import static org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.ENGINE_TYPE;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.ReferenceFieldVisitor;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.UnsupportedScriptException;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchBoolPrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhrasePrefixQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MultiMatchQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryStringQuery;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.SimpleQueryStringQuery;
import org.opensearch.sql.opensearch.storage.serialization.RelJsonSerializer;

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
    requireNonNull(expression, "expression");
    try {
      // visits expression tree
      QueryExpression queryExpression =
          (QueryExpression) expression.accept(new Visitor(schema, fieldTypes, rowType, cluster));

      if (queryExpression != null && queryExpression.isPartial()) {
        throw new UnsupportedOperationException(
            "Can't handle partial QueryExpression: " + queryExpression);
      }
      return queryExpression != null ? queryExpression.builder() : null;
    } catch (PredicateAnalyzerException | UnsupportedOperationException e) {
      return new ScriptQueryExpression(expression, rowType, fieldTypes, cluster).builder();
    }
  }

  /** Traverses {@link RexNode} tree and builds OpenSearch query. */
  private static class Visitor extends RexVisitorImpl<Expression> {

    List<String> schema;
    Map<String, ExprType> fieldTypes;
    RelDataType rowType;
    RelOptCluster cluster;

    private Visitor(
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
            case LIKE, CONTAINS -> binary(call);
            default -> {
              String message = format(Locale.ROOT, "Unsupported call: [%s]", call);
              throw new PredicateAnalyzerException(message);
            }
          };
        case FUNCTION:
          return visitRelevanceFunc(call);
        default:
          String message =
              format(Locale.ROOT, "Unsupported syntax [%s] for call: [%s]", syntax, call);
          throw new PredicateAnalyzerException(message);
      }
    }

    private QueryExpression visitRelevanceFunc(RexCall call) {
      String funcName = call.getOperator().getName().toLowerCase(Locale.ROOT);
      List<RexNode> ops = call.getOperands();
      assert ops.size() >= 2 : "Relevance query function should at least have 2 operands";

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
        RexCall fieldsRexCall = (RexCall) AliasPair.from(ops.get(0), funcName).value;
        String queryLiteralOperand =
            ((LiteralExpression)
                    visitList(List.of(AliasPair.from(ops.get(1), funcName).value)).get(0))
                .stringValue();
        Map<String, String> optionalArguments =
            parseRelevanceFunctionOptionalArguments(ops, funcName);

        return MULTI_FIELDS_RELEVANCE_FUNCTION_HANDLERS
            .get(funcName)
            .apply(fieldsRexCall, queryLiteralOperand, optionalArguments);
      }

      throw new PredicateAnalyzerException(
          String.format(Locale.ROOT, "Unsupported search relevance function: [%s]", funcName));
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
      Map<String, String> optionalArguments = new HashMap<>();

      for (int i = 2; i < operands.size(); i++) {
        AliasPair aliasPair = AliasPair.from(operands.get(i), funcName);
        String key = ((RexLiteral) aliasPair.alias).getValueAs(String.class);
        if (optionalArguments.containsKey(key)) {
          throw new PredicateAnalyzerException(
              String.format(
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
            String.format(
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
      checkArgument(call.getKind() == SqlKind.IS_NULL || call.getKind() == SqlKind.IS_NOT_NULL);
      if (call.getOperands().size() != 1) {
        String message = format(Locale.ROOT, "Unsupported operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }
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
        case LIKE:
          throw new UnsupportedOperationException("LIKE not yet supported");
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
                                    .equals(range.lowerEndpoint(), isTimeStamp)
                                : QueryExpression.create(pair.getKey()).between(range, isTimeStamp))
                    .toList();
            if (queryExpressions.size() == 1) {
              return queryExpressions.getFirst();
            } else {
              return CompoundQueryExpression.or(queryExpressions.toArray(new QueryExpression[0]));
            }
          }
        default:
          break;
      }
      String message = format(Locale.ROOT, "Unable to handle call: [%s]", call);
      throw new PredicateAnalyzerException(message);
    }

    private QueryExpression andOr(RexCall call) {
      QueryExpression[] expressions = new QueryExpression[call.getOperands().size()];
      boolean partial = false;
      // For function isEmpty and isBlank, we implement them via expression `isNull or {@function}`,
      // Unlike `OR` in Java, `SHOULD` in DSL will evaluate both branches and lead to NPE.
      if (call.getKind() == SqlKind.OR
          && call.getOperands().size() == 2
          && (call.getOperands().get(0).getKind() == SqlKind.IS_NULL
              || call.getOperands().get(1).getKind() == SqlKind.IS_NULL)) {
        throw new UnsupportedScriptException(
            "DSL will evaluate both branches of OR with isNUll, prevent push-down to avoid NPE");
      }
      for (int i = 0; i < call.getOperands().size(); i++) {
        try {
          Expression expr = call.getOperands().get(i).accept(this);
          if (expr instanceof NamedFieldExpression) {
            // nop currently
          } else {
            expressions[i] = (QueryExpression) expr;
          }
          partial |= expressions[i].isPartial();
        } catch (PredicateAnalyzerException e) {
          try {
            expressions[i] =
                new ScriptQueryExpression(call.getOperands().get(i), rowType, fieldTypes, cluster);
          } catch (UnsupportedScriptException ex) {
            if (call.getKind() == SqlKind.OR) throw ex;
            partial = true;
          }
        } catch (UnsupportedScriptException e) {
          if (call.getKind() == SqlKind.OR) throw e;
          partial = true;
        }
      }

      switch (call.getKind()) {
        case OR:
          return CompoundQueryExpression.or(expressions);
        case AND:
          return CompoundQueryExpression.and(partial, expressions);
        default:
          String message = format(Locale.ROOT, "Unable to handle call: [%s]", call);
          throw new PredicateAnalyzerException(message);
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
  abstract static class QueryExpression implements Expression {

    public abstract QueryBuilder builder();

    public boolean isPartial() {
      return false;
    }

    public abstract QueryExpression contains(LiteralExpression literal);

    /** Negate {@code this} QueryExpression (not the next one). */
    public abstract QueryExpression not();

    public abstract QueryExpression exists();

    public abstract QueryExpression notExists();

    public abstract QueryExpression like(LiteralExpression literal);

    public abstract QueryExpression notLike(LiteralExpression literal);

    public abstract QueryExpression equals(LiteralExpression literal);

    public abstract QueryExpression in(LiteralExpression literal);

    public abstract QueryExpression notIn(LiteralExpression literal);

    public QueryExpression between(Range<?> literal, boolean isTimeStamp) {
      throw new PredicateAnalyzer.PredicateAnalyzerException(
          "between cannot be applied to " + this.getClass());
    }

    public QueryExpression equals(Object point, boolean isTimeStamp) {
      throw new PredicateAnalyzer.PredicateAnalyzerException(
          "equals cannot be applied to " + this.getClass());
    }

    public abstract QueryExpression notEquals(LiteralExpression literal);

    public abstract QueryExpression gt(LiteralExpression literal);

    public abstract QueryExpression gte(LiteralExpression literal);

    public abstract QueryExpression lt(LiteralExpression literal);

    public abstract QueryExpression lte(LiteralExpression literal);

    public abstract QueryExpression match(String query, Map<String, String> optionalArguments);

    public abstract QueryExpression matchPhrase(
        String query, Map<String, String> optionalArguments);

    public abstract QueryExpression matchBoolPrefix(
        String query, Map<String, String> optionalArguments);

    public abstract QueryExpression matchPhrasePrefix(
        String query, Map<String, String> optionalArguments);

    public abstract QueryExpression simpleQueryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments);

    public abstract QueryExpression queryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments);

    public abstract QueryExpression multiMatch(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments);

    public abstract QueryExpression isTrue();

    public static QueryExpression create(TerminalExpression expression) {
      if (expression instanceof CastExpression) {
        expression = CastExpression.unpack(expression);
      }

      if (expression instanceof NamedFieldExpression) {
        return new SimpleQueryExpression((NamedFieldExpression) expression);
      } else {
        String message = format(Locale.ROOT, "Unsupported expression: [%s]", expression);
        throw new PredicateAnalyzer.PredicateAnalyzerException(message);
      }
    }
  }

  /** Builds conjunctions / disjunctions based on existing expressions. */
  static class CompoundQueryExpression extends QueryExpression {

    private final boolean partial;
    private final BoolQueryBuilder builder;

    public static CompoundQueryExpression or(QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(false);
      for (QueryExpression expression : expressions) {
        bqe.builder.should(expression.builder());
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
        if (expression != null) { // partial expressions have nulls for missing nodes
          bqe.builder.must(expression.builder());
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
    public QueryExpression not() {
      return new CompoundQueryExpression(partial, boolQuery().mustNot(builder()));
    }

    @Override
    public QueryExpression exists() {
      throw new PredicateAnalyzer.PredicateAnalyzerException(
          "SqlOperatorImpl ['exists'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression contains(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['contains'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notExists() {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['notExists'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression like(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['like'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notLike(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['notLike'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['='] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['not'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['>'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['>='] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['<'] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['<='] " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression match(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException("Match " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression matchPhrase(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchPhrase " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression matchBoolPrefix(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchBoolPrefix " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression matchPhrasePrefix(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchPhrasePrefix " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression simpleQueryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "SimpleQueryString " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression queryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "QueryString " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression multiMatch(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MultiMatch " + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression isTrue() {
      throw new PredicateAnalyzerException("isTrue cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression in(LiteralExpression literal) {
      throw new PredicateAnalyzerException("in cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notIn(LiteralExpression literal) {
      throw new PredicateAnalyzerException("notIn cannot be applied to a compound expression");
    }
  }

  /** Usually basic expression of type {@code a = 'val'} or {@code b > 42}. */
  static class SimpleQueryExpression extends QueryExpression {

    private final NamedFieldExpression rel;
    private QueryBuilder builder;

    private String getFieldReference() {
      return rel.getReference();
    }

    private String getFieldReferenceForTermQuery() {
      return rel.getReferenceForTermQuery();
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

    @Override
    public QueryExpression like(LiteralExpression literal) {
      builder = regexpQuery(getFieldReference(), literal.stringValue());
      return this;
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
      builder = termQuery(getFieldReferenceForTermQuery(), true);
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
      value = (value instanceof NlsString nls) ? nls.getValue() : value;
      return isTimeStamp ? timestampValueForPushDown(value.toString()) : value;
    }
  }

  private static String timestampValueForPushDown(String value) {
    ExprTimestampValue exprTimestampValue = new ExprTimestampValue(value);
    return DateFieldMapper.getDefaultDateTimeFormatter()
        .format(exprTimestampValue.timestampValue());
    // https://github.com/opensearch-project/sql/pull/3442
  }

  static class ScriptQueryExpression extends QueryExpression {
    private final String code;

    public ScriptQueryExpression(
        RexNode rexNode,
        RelDataType rowType,
        Map<String, ExprType> fieldTypes,
        RelOptCluster cluster) {
      ReferenceFieldVisitor validator = new ReferenceFieldVisitor(rowType, fieldTypes, true);
      // Dry run visitInputRef to make sure the input reference ExprType is valid for script
      // pushdown
      validator.visitEach(List.of(rexNode));
      RelJsonSerializer serializer = new RelJsonSerializer(cluster);
      this.code = serializer.serialize(rexNode, rowType, fieldTypes);
    }

    @Override
    public QueryBuilder builder() {
      long currentTime = Hook.CURRENT_TIME.get(-1L);
      if (currentTime < 0) {
        throw new UnsupportedScriptException(
            "ScriptQueryExpression requires a valid current time from hook, but it is not set");
      }
      return new ScriptQueryBuilder(
          new Script(
              DEFAULT_SCRIPT_TYPE,
              COMPOUNDED_LANG_NAME,
              code,
              Map.of(ENGINE_TYPE, CALCITE_ENGINE_TYPE),
              Map.of(Variable.UTC_TIMESTAMP.camelName, currentTime)));
    }

    @Override
    public QueryExpression exists() {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['exists'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression contains(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['contains'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression not() {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['not'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression notExists() {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['notExists'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression like(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['like'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression notLike(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['notLike'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['='] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['not'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['>'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['>='] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['<'] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      throw new PredicateAnalyzerException(
          "SqlOperatorImpl ['<='] " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression match(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "Match query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression matchPhrase(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchPhrase query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression matchBoolPrefix(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchBoolPrefix query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression matchPhrasePrefix(String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MatchPhrasePrefix query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression simpleQueryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "SimpleQueryString query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression queryString(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "QueryString query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression multiMatch(
        RexCall fieldsRexCall, String query, Map<String, String> optionalArguments) {
      throw new PredicateAnalyzerException(
          "MultiMatch query " + "cannot be applied to a script expression");
    }

    @Override
    public QueryExpression isTrue() {
      throw new PredicateAnalyzerException("isTrue cannot be applied to a script expression");
    }

    @Override
    public QueryExpression in(LiteralExpression literal) {
      throw new PredicateAnalyzerException("in cannot be applied to a script expression");
    }

    @Override
    public QueryExpression notIn(LiteralExpression literal) {
      throw new PredicateAnalyzerException("notIn cannot be applied to a script expression");
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

    String getRootName() {
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

    String toKeywordSubField() {
      ExprType type = this.type.getOriginalExprType();
      if (type instanceof OpenSearchTextType) {
        OpenSearchTextType textType = (OpenSearchTextType) type;
        // For OpenSearch Alias type which maps to the field of text type,
        // we have to use its original path
        String path = this.type.getOriginalPath().orElse(this.name);
        // Find the first subfield with type keyword, return null if non-exist.
        return textType.getFields().entrySet().stream()
            .filter(e -> e.getValue().getMappingType() == MappingType.Keyword)
            .findFirst()
            .map(e -> path + "." + e.getKey())
            .orElse(null);
      }
      return null;
    }

    boolean isMetaField() {
      return OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(getRootName());
    }

    String getReference() {
      return getRootName();
    }

    public String getReferenceForTermQuery() {
      if (isTextType()) {
        return toKeywordSubField();
      }
      return getRootName();
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
        return exprSqlType.getUdt() == OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP;
      }
      return false;
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
      final RelDataType type = literal.getType();
      List<Object> values = new ArrayList<>();
      final SqlTypeName sqlTypeName = type.getSqlTypeName();
      if (sarg.isPoints()) {
        Set<Range> ranges = sarg.rangeSet.asRanges();
        ranges.forEach(range -> values.add(sargPointValue(range.lowerEndpoint(), sqlTypeName)));
      } else if (sarg.isComplementedPoints()) {
        Set<Range> ranges = sarg.negate().rangeSet.asRanges();
        ranges.forEach(range -> values.add(sargPointValue(range.lowerEndpoint(), sqlTypeName)));
      }
      return values;
    }

    Object sargPointValue(Object point, SqlTypeName sqlTypeName) {
      switch (sqlTypeName) {
        case CHAR:
        case VARCHAR:
          return ((NlsString) point).getValue();
        case DECIMAL:
          return ((BigDecimal) point).doubleValue();
        default:
          return point;
      }
    }

    Object rawValue() {
      return literal.getValue();
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
      throw new PredicateAnalyzer.PredicateAnalyzerException(
          "Cannot handle " + call.getKind() + " expression for _id field, " + call);
    }
  }
}
