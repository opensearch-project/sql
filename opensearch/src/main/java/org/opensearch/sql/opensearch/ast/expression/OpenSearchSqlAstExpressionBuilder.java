/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.ast.expression;

import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NestedExpressionAtomContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NestedAllFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.HighlightFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScoreRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NoFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SingleFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AltSingleFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.MultiFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AltMultiFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AlternateMultiMatchQueryContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RelevanceArgContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AlternateMultiMatchFieldContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RelevanceFieldAndWeightContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.sql.parser.AstExpressionBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Extension for SQL AstExpressionBuilder. */
// TODO make opensearch parser and grammar files
// TODO make merge SQl and PPL AST builders (probably, inherit from ParseTreeVisitor<UnresolvedExpression>)
public class OpenSearchSqlAstExpressionBuilder extends AstExpressionBuilder {

  public OpenSearchSqlAstExpressionBuilder() {
    super(null);
  }

  @Override
  public UnresolvedExpression visitNestedExpressionAtom(NestedExpressionAtomContext ctx) {
    return visit(ctx.expression()); // Discard parenthesis around
  }

  @Override
  public UnresolvedExpression visitNestedAllFunctionCall(NestedAllFunctionCallContext ctx) {
    return new NestedAllTupleFields(
        visitQualifiedName(ctx.allTupleFields().path).toString()
    );
  }


  @Override
  public UnresolvedExpression visitHighlightFunctionCall(HighlightFunctionCallContext ctx) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.highlightFunction().highlightArg().forEach(v -> builder.put(
        v.highlightArgName().getText().toLowerCase(),
        new Literal(StringUtils.unquoteText(v.highlightArgValue().getText()),
            DataType.STRING))
    );

    return new HighlightFunction(visit(ctx.highlightFunction().relevanceField()),
        builder.build());
  }

  @Override
  public UnresolvedExpression visitScoreRelevanceFunction(ScoreRelevanceFunctionContext ctx) {
    Literal weight =
        ctx.weight == null
            ? new Literal(Double.valueOf(1.0), DataType.DOUBLE)
            : new Literal(Double.parseDouble(ctx.weight.getText()), DataType.DOUBLE);
    return new ScoreFunction(visit(ctx.relevanceFunction()), weight);
  }

  @Override
  public UnresolvedExpression visitNoFieldRelevanceFunction(NoFieldRelevanceFunctionContext ctx) {
    return new Function(
            ctx.noFieldRelevanceFunctionName().getText().toLowerCase(),
            noFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitSingleFieldRelevanceFunction(SingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.singleFieldRelevanceFunctionName().getText().toLowerCase(),
        singleFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitAltSingleFieldRelevanceFunction(AltSingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.altSyntaxFunctionName.getText().toLowerCase(),
        altSingleFieldRelevanceFunctionArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitMultiFieldRelevanceFunction(MultiFieldRelevanceFunctionContext ctx) {
    // To support alternate syntax for MULTI_MATCH like
    // 'MULTI_MATCH('query'='query_val', 'fields'='*fields_val')'
    String funcName = StringUtils.unquoteText(ctx.multiFieldRelevanceFunctionName().getText());
    if ((funcName.equalsIgnoreCase(BuiltinFunctionName.MULTI_MATCH.toString())
        || funcName.equalsIgnoreCase(BuiltinFunctionName.MULTIMATCH.toString())
        || funcName.equalsIgnoreCase(BuiltinFunctionName.MULTIMATCHQUERY.toString()))
        && !ctx.getRuleContexts(AlternateMultiMatchQueryContext.class)
        .isEmpty()) {
      return new Function(
          ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(),
          alternateMultiMatchArguments(ctx));
    } else {
      return new Function(
          ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(),
          multiFieldRelevanceArguments(ctx));
    }
  }

  @Override
  public UnresolvedExpression visitAltMultiFieldRelevanceFunction(AltMultiFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.altSyntaxFunctionName.getText().toLowerCase(),
        altMultiFieldRelevanceFunctionArguments(ctx));
  }

  private void fillRelevanceArgs(List<RelevanceArgContext> args,
                                 ImmutableList.Builder<UnresolvedExpression> builder) {
    // To support old syntax we must support argument keys as quoted strings.
    args.forEach(v -> builder.add(v.argName == null
        ? new UnresolvedArgument(v.relevanceArgName().getText().toLowerCase(),
            new Literal(StringUtils.unquoteText(v.relevanceArgValue().getText()),
            DataType.STRING))
        : new UnresolvedArgument(StringUtils.unquoteText(v.argName.getText()).toLowerCase(),
            new Literal(StringUtils.unquoteText(v.argVal.getText()), DataType.STRING))));
  }

  private List<UnresolvedExpression> noFieldRelevanceArguments(NoFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("query",
            new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  private List<UnresolvedExpression> singleFieldRelevanceArguments(SingleFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("field",
        new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }


  private List<UnresolvedExpression> altSingleFieldRelevanceFunctionArguments(AltSingleFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("field",
        new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  private List<UnresolvedExpression> multiFieldRelevanceArguments(MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    var fields = new RelevanceFieldList(ctx
        .getRuleContexts(RelevanceFieldAndWeightContext.class)
        .stream()
        .collect(Collectors.toMap(
            f -> StringUtils.unquoteText(f.field.getText()),
            f -> (f.weight == null) ? 1F : Float.parseFloat(f.weight.getText()))));
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  /**
   * Adds support for multi_match alternate syntax like
   * MULTI_MATCH('query'='Dale', 'fields'='*name').
   *
   * @param ctx : Context for multi field relevance function.
   * @return : Returns list of all arguments for relevance function.
   */
  private List<UnresolvedExpression> alternateMultiMatchArguments(MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    Map<String, Float> fieldAndWeightMap = new HashMap<>();

    String[] fieldAndWeights = StringUtils.unquoteText(
        ctx.getRuleContexts(AlternateMultiMatchFieldContext.class)
                .stream().findFirst().get().argVal.getText()).split(",");

    for (var fieldAndWeight : fieldAndWeights) {
      String[] splitFieldAndWeights = fieldAndWeight.split("\\^");
      fieldAndWeightMap.put(splitFieldAndWeights[0],
          splitFieldAndWeights.length > 1 ? Float.parseFloat(splitFieldAndWeights[1]) : 1F);
    }
    builder.add(new UnresolvedArgument("fields",
        new RelevanceFieldList(fieldAndWeightMap)));

    ctx.getRuleContexts(AlternateMultiMatchQueryContext.class)
        .stream().findFirst().ifPresent(
              arg ->
                    builder.add(new UnresolvedArgument("query",
                        new Literal(
                            StringUtils.unquoteText(arg.argVal.getText()), DataType.STRING)))
        );

    fillRelevanceArgs(ctx.relevanceArg(), builder);

    return builder.build();
  }

  private List<UnresolvedExpression> altMultiFieldRelevanceFunctionArguments(AltMultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    var map = new HashMap<String, Float>();
    map.put(ctx.field.getText(), 1F);
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    var fields = new RelevanceFieldList(map);
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }
}
