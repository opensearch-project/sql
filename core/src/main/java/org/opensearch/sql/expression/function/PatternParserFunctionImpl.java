/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.common.patterns.PatternUtils;
import org.opensearch.sql.common.patterns.PatternUtils.ParseResult;

public class PatternParserFunctionImpl extends ImplementorUDF {
  protected PatternParserFunctionImpl() {
    super(new PatternParserImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(UserDefinedFunctionUtils.patternStruct);
  }

  public static class PatternParserImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      assert call.getOperands().size() == 2 : "PATTERN_PARSER should have 2 arguments";
      assert translatedOperands.size() == 2 : "PATTERN_PARSER should have 2 arguments";

      RelDataType inputType = call.getOperands().get(1).getType();
      Method method = resolveEvaluationMethod(inputType);

      ScalarFunctionImpl function = (ScalarFunctionImpl) ScalarFunctionImpl.create(method);
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }

    private Method resolveEvaluationMethod(RelDataType inputType) {
      if (inputType.getSqlTypeName() == SqlTypeName.VARCHAR) {
        return getMethod(String.class, "evalField");
      }

      RelDataType componentType = inputType.getComponentType();
      return (componentType.getSqlTypeName() == SqlTypeName.MAP)
          ? getMethod(Object.class, "evalAgg")
          : getMethod(List.class, "evalSamples");
    }

    private Method getMethod(Class<?> paramType, String methodName) {
      return Types.lookupMethod(
          PatternParserFunctionImpl.class, methodName, String.class, paramType);
    }
  }

  /*
   * A simple and general label pattern algorithm given aggregated patterns, which is adopted from
   * Drain algorithm(see https://ieeexplore.ieee.org/document/8029742).
   */
  @Strict
  public static Object evalAgg(
      @Parameter(name = "field") String field, @Parameter(name = "aggObject") Object aggObject) {
    if (Strings.isBlank(field)) {
      return ImmutableMap.of();
    }
    List<Map<String, Object>> aggResult = (List<Map<String, Object>>) aggObject;
    List<String> preprocessedTokens =
        BrainLogParser.preprocess(
            field,
            BrainLogParser.DEFAULT_FILTER_PATTERN_VARIABLE_MAP,
            BrainLogParser.DEFAULT_DELIMITERS);
    List<List<String>> candidates =
        aggResult.stream()
            .map(m -> (String) m.get(PatternUtils.PATTERN))
            .map(pattern -> pattern.split(" "))
            .filter(splitPattern -> splitPattern.length == preprocessedTokens.size())
            .map(Arrays::asList)
            .toList();
    List<String> bestCandidate = findBestCandidate(candidates, preprocessedTokens);

    if (bestCandidate != null) {
      String bestCandidatePattern = String.join(" ", bestCandidate);
      Map<String, List<String>> tokensMap = new HashMap<>();
      ParseResult parseResult =
          PatternUtils.parsePattern(bestCandidatePattern, PatternUtils.TOKEN_PATTERN);
      PatternUtils.extractVariables(parseResult, field, tokensMap, PatternUtils.TOKEN_PREFIX);
      return ImmutableMap.of(
          PatternUtils.PATTERN, bestCandidatePattern,
          PatternUtils.TOKENS, tokensMap);
    } else {
      return ImmutableMap.of();
    }
  }

  @Strict
  public static Object evalField(
      @Parameter(name = "pattern") String pattern, @Parameter(name = "field") String field) {
    if (Strings.isBlank(field)) {
      return ImmutableMap.of();
    }

    Map<String, List<String>> tokensMap = new HashMap<>();
    ParseResult parseResult = PatternUtils.parsePattern(pattern, PatternUtils.WILDCARD_PATTERN);

    PatternUtils.extractVariables(parseResult, field, tokensMap, PatternUtils.WILDCARD_PREFIX);
    return ImmutableMap.of(
        PatternUtils.PATTERN,
        parseResult.toTokenOrderString(PatternUtils.WILDCARD_PREFIX),
        PatternUtils.TOKENS,
        tokensMap);
  }

  @Strict
  public static Object evalSamples(
      @Parameter(name = "pattern") String pattern,
      @Parameter(name = "sample_logs") List<String> sampleLogs) {
    if (sampleLogs.isEmpty()) {
      return ImmutableMap.of();
    }
    Map<String, List<String>> tokensMap = new HashMap<>();
    ParseResult parseResult = PatternUtils.parsePattern(pattern, PatternUtils.WILDCARD_PATTERN);

    for (String sampleLog : sampleLogs) {
      PatternUtils.extractVariables(
          parseResult, sampleLog, tokensMap, PatternUtils.WILDCARD_PREFIX);
    }
    return ImmutableMap.of(
        PatternUtils.PATTERN,
        parseResult.toTokenOrderString(PatternUtils.WILDCARD_PREFIX),
        PatternUtils.TOKENS,
        tokensMap);
  }

  private static List<String> findBestCandidate(
      List<List<String>> candidates, List<String> tokens) {
    return candidates.stream()
        .max(Comparator.comparingDouble(candidate -> calculateScore(tokens, candidate)))
        .orElse(null);
  }

  private static float calculateScore(List<String> tokens, List<String> candidate) {
    int score = 0;
    for (int i = 0; i < tokens.size(); i++) {
      String preprocessedToken = tokens.get(i);
      String candidateToken = candidate.get(i);
      if (Objects.equals(preprocessedToken, candidateToken)) {
        score += 1;
      } else if (preprocessedToken.startsWith("<*") && candidateToken.startsWith("<token")) {
        score += 1;
      }
    }
    return (float) score / tokens.size();
  }
}
