/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedAttribute;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.AggregationState;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.conditional.cases.CaseClause;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.expression.window.aggregation.AggregateWindowFunction;

/**
 * Analyze the {@link UnresolvedExpression} in the {@link AnalysisContext} to construct the {@link
 * Expression}.
 */
public class ExpressionAnalyzer extends AbstractNodeVisitor<Expression, AnalysisContext> {
  @Getter private final BuiltinFunctionRepository repository;

  @Override
  public Expression visitCast(Cast node, AnalysisContext context) {
    final Expression expression = node.getExpression().accept(this, context);
    return (Expression)
        repository.compile(
            context.getFunctionProperties(),
            node.convertFunctionName(),
            Collections.singletonList(expression));
  }

  public ExpressionAnalyzer(BuiltinFunctionRepository repository) {
    this.repository = repository;
  }

  public Expression analyze(UnresolvedExpression unresolved, AnalysisContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public Expression visitUnresolvedAttribute(UnresolvedAttribute node, AnalysisContext context) {
    return visitIdentifier(node.getAttr(), context);
  }

  @Override
  public Expression visitEqualTo(EqualTo node, AnalysisContext context) {
    Expression left = node.getLeft().accept(this, context);
    Expression right = node.getRight().accept(this, context);

    return DSL.equal(left, right);
  }

  @Override
  public Expression visitLiteral(Literal node, AnalysisContext context) {
    return DSL.literal(
        ExprValueUtils.fromObjectValue(node.getValue(), node.getType().getCoreType()));
  }

  @Override
  public Expression visitInterval(Interval node, AnalysisContext context) {
    Expression value = node.getValue().accept(this, context);
    Expression unit = DSL.literal(node.getUnit().name());
    return DSL.interval(value, unit);
  }

  @Override
  public Expression visitAnd(And node, AnalysisContext context) {
    Expression left = node.getLeft().accept(this, context);
    Expression right = node.getRight().accept(this, context);

    return DSL.and(left, right);
  }

  @Override
  public Expression visitOr(Or node, AnalysisContext context) {
    Expression left = node.getLeft().accept(this, context);
    Expression right = node.getRight().accept(this, context);

    return DSL.or(left, right);
  }

  @Override
  public Expression visitXor(Xor node, AnalysisContext context) {
    Expression left = node.getLeft().accept(this, context);
    Expression right = node.getRight().accept(this, context);

    return DSL.xor(left, right);
  }

  @Override
  public Expression visitNot(Not node, AnalysisContext context) {
    return DSL.not(node.getExpression().accept(this, context));
  }

  @Override
  public Expression visitAggregateFunction(AggregateFunction node, AnalysisContext context) {
    Optional<BuiltinFunctionName> builtinFunctionName =
        BuiltinFunctionName.ofAggregation(node.getFuncName());
    if (builtinFunctionName.isPresent()) {
      ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      builder.add(node.getField().accept(this, context));
      for (UnresolvedExpression arg : node.getArgList()) {
        builder.add(arg.accept(this, context));
      }
      Aggregator aggregator =
          (Aggregator)
              repository.compile(
                  context.getFunctionProperties(),
                  builtinFunctionName.get().getName(),
                  builder.build());
      aggregator.distinct(node.getDistinct());
      if (node.condition() != null) {
        aggregator.condition(analyze(node.condition(), context));
      }
      return aggregator;
    } else {
      throw new SemanticCheckException("Unsupported aggregation function " + node.getFuncName());
    }
  }

  @Override
  public Expression visitRelevanceFieldList(RelevanceFieldList node, AnalysisContext context) {
    return new LiteralExpression(
        ExprValueUtils.tupleValue(ImmutableMap.copyOf(node.getFieldList())));
  }

  @Override
  public Expression visitFunction(Function node, AnalysisContext context) {
    FunctionName functionName = FunctionName.of(node.getFuncName());
    List<Expression> arguments =
        node.getFuncArgs().stream()
            .map(
                unresolvedExpression -> {
                  var ret = analyze(unresolvedExpression, context);
                  if (ret == null) {
                    throw new UnsupportedOperationException(
                        String.format("Invalid use of expression %s", unresolvedExpression));
                  } else {
                    return ret;
                  }
                })
            .collect(Collectors.toList());
    return (Expression)
        repository.compile(context.getFunctionProperties(), functionName, arguments);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Expression visitWindowFunction(WindowFunction node, AnalysisContext context) {
    Expression expr = node.getFunction().accept(this, context);
    // Wrap regular aggregator by aggregate window function to adapt window operator use
    if (expr instanceof Aggregator) {
      return new AggregateWindowFunction((Aggregator<AggregationState>) expr);
    }
    return expr;
  }

  @Override
  public Expression visitHighlightFunction(HighlightFunction node, AnalysisContext context) {
    Expression expr = node.getHighlightField().accept(this, context);
    return new HighlightExpression(expr);
  }

  /**
   * visitScoreFunction removes the score function from the AST and replaces it with the child
   * relevance function node. If the optional boost variable is provided, the boost argument of the
   * relevance function is combined.
   *
   * @param node score function node
   * @param context analysis context for the query
   * @return resolved relevance function
   */
  public Expression visitScoreFunction(ScoreFunction node, AnalysisContext context) {
    Literal boostArg = node.getRelevanceFieldWeight();
    if (!boostArg.getType().equals(DataType.DOUBLE)) {
      throw new SemanticCheckException(
          String.format(
              "Expected boost type '%s' but got '%s'",
              DataType.DOUBLE.name(), boostArg.getType().name()));
    }
    Double thisBoostValue = ((Double) boostArg.getValue());

    // update the existing unresolved expression to add a boost argument if it doesn't exist
    // OR multiply the existing boost argument
    Function relevanceQueryUnresolvedExpr = (Function) node.getRelevanceQuery();
    List<UnresolvedExpression> relevanceFuncArgs = relevanceQueryUnresolvedExpr.getFuncArgs();

    boolean doesFunctionContainBoostArgument = false;
    List<UnresolvedExpression> updatedFuncArgs = new ArrayList<>();
    for (UnresolvedExpression expr : relevanceFuncArgs) {
      String argumentName = ((UnresolvedArgument) expr).getArgName();
      if (argumentName.equalsIgnoreCase("boost")) {
        doesFunctionContainBoostArgument = true;
        Literal boostArgLiteral = (Literal) ((UnresolvedArgument) expr).getValue();
        Double boostValue =
            Double.parseDouble((String) boostArgLiteral.getValue()) * thisBoostValue;
        UnresolvedArgument newBoostArg =
            new UnresolvedArgument(
                argumentName, new Literal(boostValue.toString(), DataType.STRING));
        updatedFuncArgs.add(newBoostArg);
      } else {
        updatedFuncArgs.add(expr);
      }
    }

    // since nothing was found, add an argument
    if (!doesFunctionContainBoostArgument) {
      UnresolvedArgument newBoostArg =
          new UnresolvedArgument(
              "boost", new Literal(Double.toString(thisBoostValue), DataType.STRING));
      updatedFuncArgs.add(newBoostArg);
    }

    // create a new function expression with boost argument and resolve it
    Function updatedRelevanceQueryUnresolvedExpr =
        new Function(relevanceQueryUnresolvedExpr.getFuncName(), updatedFuncArgs);
    OpenSearchFunctions.OpenSearchFunction relevanceQueryExpr =
        (OpenSearchFunctions.OpenSearchFunction)
            updatedRelevanceQueryUnresolvedExpr.accept(this, context);
    relevanceQueryExpr.setScoreTracked(true);
    return relevanceQueryExpr;
  }

  @Override
  public Expression visitIn(In node, AnalysisContext context) {
    return visitIn(node.getField(), node.getValueList(), context);
  }

  private Expression visitIn(
      UnresolvedExpression field, List<UnresolvedExpression> valueList, AnalysisContext context) {
    if (valueList.size() == 1) {
      return visitCompare(new Compare("=", field, valueList.get(0)), context);
    } else if (valueList.size() > 1) {
      return DSL.or(
          visitCompare(new Compare("=", field, valueList.get(0)), context),
          visitIn(field, valueList.subList(1, valueList.size()), context));
    } else {
      throw new SemanticCheckException("Values in In clause should not be empty");
    }
  }

  @Override
  public Expression visitCompare(Compare node, AnalysisContext context) {
    FunctionName functionName = FunctionName.of(node.getOperator());
    Expression left = analyze(node.getLeft(), context);
    Expression right = analyze(node.getRight(), context);
    return (Expression)
        repository.compile(
            context.getFunctionProperties(), functionName, Arrays.asList(left, right));
  }

  @Override
  public Expression visitBetween(Between node, AnalysisContext context) {
    return and(
            compare(">=", node.getValue(), node.getLowerBound()),
            compare("<=", node.getValue(), node.getUpperBound()))
        .accept(this, context);
  }

  @Override
  public Expression visitCase(Case node, AnalysisContext context) {
    List<WhenClause> whens = new ArrayList<>();
    for (When when : node.getWhenClauses()) {
      if (node.getCaseValue() == null) {
        whens.add((WhenClause) analyze(when, context));
      } else {
        // Merge case value and condition (compare value) into a single equal condition
        whens.add(
            (WhenClause)
                analyze(
                    new When(
                        new Function("=", Arrays.asList(node.getCaseValue(), when.getCondition())),
                        when.getResult()),
                    context));
      }
    }

    Expression defaultResult =
        (node.getElseClause() == null) ? null : analyze(node.getElseClause(), context);
    CaseClause caseClause = new CaseClause(whens, defaultResult);

    // To make this simple, require all result type same regardless of implicit convert
    // Make CaseClause return list so it can be used in error message in determined order
    List<ExprType> resultTypes = caseClause.allResultTypes();
    if (ImmutableSet.copyOf(resultTypes).size() > 1) {
      throw new SemanticCheckException(
          "All result types of CASE clause must be the same, but found " + resultTypes);
    }
    return caseClause;
  }

  @Override
  public Expression visitWhen(When node, AnalysisContext context) {
    return new WhenClause(
        analyze(node.getCondition(), context), analyze(node.getResult(), context));
  }

  @Override
  public Expression visitField(Field node, AnalysisContext context) {
    String attr = node.getField().toString();
    return visitIdentifier(attr, context);
  }

  @Override
  public Expression visitAllFields(AllFields node, AnalysisContext context) {
    // Convert to string literal for argument in COUNT(*), because there is no difference between
    // COUNT(*) and COUNT(literal). For SELECT *, its select expression analyzer will expand * to
    // the right field name list by itself.
    return DSL.literal("*");
  }

  @Override
  public Expression visitQualifiedName(QualifiedName node, AnalysisContext context) {
    QualifierAnalyzer qualifierAnalyzer = new QualifierAnalyzer(context);

    // check for reserved words in the identifier
    for (String part : node.getParts()) {
      for (TypeEnvironment typeEnv = context.peek();
          typeEnv != null;
          typeEnv = typeEnv.getParent()) {
        Optional<ExprType> exprType =
            typeEnv.getReservedSymbolTable().lookup(new Symbol(Namespace.FIELD_NAME, part));
        if (exprType.isPresent()) {
          return visitMetadata(
              qualifierAnalyzer.unqualified(node), (ExprCoreType) exprType.get(), context);
        }
      }
    }
    return visitIdentifier(qualifierAnalyzer.unqualified(node), context);
  }

  @Override
  public Expression visitSpan(Span node, AnalysisContext context) {
    return new SpanExpression(
        node.getField().accept(this, context),
        node.getValue().accept(this, context),
        node.getUnit());
  }

  @Override
  public Expression visitUnresolvedArgument(UnresolvedArgument node, AnalysisContext context) {
    return new NamedArgumentExpression(node.getArgName(), node.getValue().accept(this, context));
  }

  /**
   * If QualifiedName is actually a reserved metadata field, return the expr type associated with
   * the metadata field.
   *
   * @param ident metadata field name
   * @param context analysis context
   * @return DSL reference
   */
  private Expression visitMetadata(
      String ident, ExprCoreType exprCoreType, AnalysisContext context) {
    return DSL.ref(ident, exprCoreType);
  }

  private Expression visitIdentifier(String ident, AnalysisContext context) {
    // ParseExpression will always override ReferenceExpression when ident conflicts
    for (NamedExpression expr : context.getNamedParseExpressions()) {
      if (expr.getNameOrAlias().equals(ident) && expr.getDelegated() instanceof ParseExpression) {
        return expr.getDelegated();
      }
    }

    TypeEnvironment typeEnv = context.peek();
    ReferenceExpression ref =
        DSL.ref(ident, typeEnv.resolve(new Symbol(Namespace.FIELD_NAME, ident)));

    return ref;
  }
}
