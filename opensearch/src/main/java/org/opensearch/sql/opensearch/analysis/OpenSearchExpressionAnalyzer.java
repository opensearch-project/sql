package org.opensearch.sql.opensearch.analysis;

import com.google.common.collect.ImmutableMap;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.OpenSearchFunctions;

import java.util.ArrayList;
import java.util.List;

public class OpenSearchExpressionAnalyzer extends ExpressionAnalyzer {
  public OpenSearchExpressionAnalyzer(BuiltinFunctionRepository repository) {
    super(repository);
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
  public Expression visitRelevanceFieldList(RelevanceFieldList node, AnalysisContext context) {
    return new LiteralExpression(
        ExprValueUtils.tupleValue(ImmutableMap.copyOf(node.getFieldList())));
  }
}
