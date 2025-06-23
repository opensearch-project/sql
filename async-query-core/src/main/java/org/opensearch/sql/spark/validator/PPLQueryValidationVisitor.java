/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.opensearch.sql.spark.antlr.parser.OpenSearchPPLParser.*;

@AllArgsConstructor
public class PPLQueryValidationVisitor
    extends org.opensearch.sql.spark.antlr.parser.OpenSearchPPLParserBaseVisitor<Void> {
  private final GrammarElementValidator grammarElementValidator;

  @Override
  public Void visitPatternsCommand(PatternsCommandContext ctx) {
    validateAllowed(PPLGrammarElement.PATTERNS_COMMAND);
    return super.visitPatternsCommand(ctx);
  }

  @Override
  public Void visitJoinCommand(JoinCommandContext ctx) {
    validateAllowed(PPLGrammarElement.JOIN_COMMAND);
    return super.visitJoinCommand(ctx);
  }

  @Override
  public Void visitLookupCommand(LookupCommandContext ctx) {
    validateAllowed(PPLGrammarElement.LOOKUP_COMMAND);
    return super.visitLookupCommand(ctx);
  }

  @Override
  public Void visitSubSearch(SubSearchContext ctx) {
    validateAllowed(PPLGrammarElement.SUBQUERY_COMMAND);
    return super.visitSubSearch(ctx);
  }

  @Override
  public Void visitFlattenCommand(FlattenCommandContext ctx) {
    validateAllowed(PPLGrammarElement.FLATTEN_COMMAND);
    return super.visitFlattenCommand(ctx);
  }

  @Override
  public Void visitFillnullCommand(FillnullCommandContext ctx) {
    validateAllowed(PPLGrammarElement.FILLNULL_COMMAND);
    return super.visitFillnullCommand(ctx);
  }

  @Override
  public Void visitExpandCommand(ExpandCommandContext ctx) {
    validateAllowed(PPLGrammarElement.EXPAND_COMMAND);
    return super.visitExpandCommand(ctx);
  }

  @Override
  public Void visitDescribeCommand(DescribeCommandContext ctx) {
    validateAllowed(PPLGrammarElement.DESCRIBE_COMMAND);
    return super.visitDescribeCommand(ctx);
  }

  @Override
  public Void visitCidrMatchFunctionCall(CidrMatchFunctionCallContext ctx) {
    validateAllowed(PPLGrammarElement.IPADDRESS_FUNCTIONS);
    return super.visitCidrMatchFunctionCall(ctx);
  }

  @Override
  public Void visitJsonFunctionName(JsonFunctionNameContext ctx) {
    validateAllowed(PPLGrammarElement.JSON_FUNCTIONS);
    return super.visitJsonFunctionName(ctx);
  }

  @Override
  public Void visitLambdaFunctionName(LambdaFunctionNameContext ctx) {
    validateAllowed(PPLGrammarElement.LAMBDA_FUNCTIONS);
    return super.visitLambdaFunctionName(ctx);
  }

  private void validateAllowed(PPLGrammarElement element) {
    if (!grammarElementValidator.isValid(element)) {
      throw new IllegalArgumentException(element + " is not allowed.");
    }
  }
}
