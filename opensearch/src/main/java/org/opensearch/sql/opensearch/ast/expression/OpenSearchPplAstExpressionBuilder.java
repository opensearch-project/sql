/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;

import java.util.List;
import java.util.stream.Collectors;

public class OpenSearchPplAstExpressionBuilder extends AstExpressionBuilder {

  public OpenSearchPplAstExpressionBuilder() {
    super(null);
  }

  @Override
  public UnresolvedExpression visitSingleFieldRelevanceFunction(
      OpenSearchPPLParser.SingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.singleFieldRelevanceFunctionName().getText().toLowerCase(),
        singleFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitMultiFieldRelevanceFunction(
      OpenSearchPPLParser.MultiFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(),
        multiFieldRelevanceArguments(ctx));
  }


  private List<UnresolvedExpression> singleFieldRelevanceArguments(
      OpenSearchPPLParser.SingleFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("field",
        new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg().forEach(v -> builder.add(new UnresolvedArgument(
        v.relevanceArgName().getText().toLowerCase(), new Literal(StringUtils.unquoteText(
        v.relevanceArgValue().getText()), DataType.STRING))));
    return builder.build();
  }

  private List<UnresolvedExpression> multiFieldRelevanceArguments(
      OpenSearchPPLParser.MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    var fields = new RelevanceFieldList(ctx
        .getRuleContexts(OpenSearchPPLParser.RelevanceFieldAndWeightContext.class)
        .stream()
        .collect(Collectors.toMap(
            f -> StringUtils.unquoteText(f.field.getText()),
            f -> (f.weight == null) ? 1F : Float.parseFloat(f.weight.getText()))));
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg().forEach(v -> builder.add(new UnresolvedArgument(
        v.relevanceArgName().getText().toLowerCase(), new Literal(StringUtils.unquoteText(
        v.relevanceArgValue().getText()), DataType.STRING))));
    return builder.build();
  }
}
