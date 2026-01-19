/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.common.patterns.PatternUtils;
import org.opensearch.sql.common.patterns.PatternUtils.ParseResult;

public class PatternParserFunctionImpl extends ImplementorUDF {
  private static final Map<String, Object> EMPTY_RESULT =
      ImmutableMap.of(PatternUtils.PATTERN, "", PatternUtils.TOKENS, Collections.emptyMap());

  protected PatternParserFunctionImpl() {
    super(new PatternParserImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(UserDefinedFunctionUtils.patternStruct);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ARRAY, SqlTypeFamily.BOOLEAN)
                .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER))
                .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ARRAY)));
  }

  public static class PatternParserImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      int operandCount = call.getOperands().size();
      int translatedOperandCount = translatedOperands.size();
      assert operandCount == 3 || operandCount == 2 : "PATTERN_PARSER should have 2 or 3 arguments";
      assert translatedOperandCount == 3 || translatedOperandCount == 2
          : "PATTERN_PARSER should have 2 or 3 arguments";

      RelDataType inputType = call.getOperands().get(1).getType();
      Method method = resolveEvaluationMethod(inputType, operandCount);

      ScalarFunctionImpl function = (ScalarFunctionImpl) ScalarFunctionImpl.create(method);
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }

    private Method resolveEvaluationMethod(RelDataType inputType, int operandCount) {
      if (inputType.getSqlTypeName() == SqlTypeName.VARCHAR) {
        return getMethod(String.class, "evalField");
      }

      RelDataType componentType = inputType.getComponentType();
      if (componentType.getSqlTypeName() == SqlTypeName.MAP) {
        // evalAgg: for label mode with aggregation results (array of maps)
        return Types.lookupMethod(
            PatternParserFunctionImpl.class, "evalAgg", String.class, Objects.class, Boolean.class);
      } else if (operandCount == 3) {
        // evalAggSamples: for UDAF pushdown aggregation mode
        // Takes pattern (String), sample_logs (List<String>), showNumberedToken (Boolean)
        return Types.lookupMethod(
            PatternParserFunctionImpl.class,
            "evalAggSamples",
            String.class,
            List.class,
            Boolean.class);
      } else {
        // evalSamples: for simple pattern with sample logs (2 arguments)
        return getMethod(List.class, "evalSamples");
      }
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
  public static Object evalAgg(
      @Parameter(name = "field") String field,
      @Parameter(name = "aggObject") Object aggObject,
      @Parameter(name = "showNumberedToken") Boolean showNumberedToken) {
    if (Strings.isBlank(field) || aggObject == null) {
      return EMPTY_RESULT;
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
      String outputPattern = bestCandidatePattern; // Default: return as-is

      if (showNumberedToken) {
        // Parse pattern with wildcard format (<*>, <*IP*>, etc.)
        // LogPatternAggFunction.value() returns patterns in wildcard format
        ParseResult parseResult =
            PatternUtils.parsePattern(bestCandidatePattern, PatternUtils.WILDCARD_PATTERN);

        // Transform pattern from wildcards to numbered tokens (<token1>, <token2>, etc.)
        outputPattern = parseResult.toTokenOrderString(PatternUtils.TOKEN_PREFIX);

        // Extract token values from the field
        PatternUtils.extractVariables(parseResult, field, tokensMap, PatternUtils.TOKEN_PREFIX);
      }

      return ImmutableMap.of(
          PatternUtils.PATTERN, outputPattern,
          PatternUtils.TOKENS, tokensMap);
    } else {
      return ImmutableMap.of();
    }
  }

  public static Object evalField(
      @Parameter(name = "pattern") String pattern, @Parameter(name = "field") String field) {
    if (Strings.isBlank(field)) {
      return EMPTY_RESULT;
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

  public static Object evalSamples(
      @Parameter(name = "pattern") String pattern,
      @Parameter(name = "sample_logs") List<String> sampleLogs) {
    if (Strings.isBlank(pattern)) {
      return EMPTY_RESULT;
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

  /**
   * Extract tokens from aggregated pattern and sample logs for UDAF pushdown. Transforms the
   * pattern from wildcard format (e.g., &lt;*&gt;) to numbered token format (e.g., &lt;token1&gt;,
   * &lt;token2&gt;) when showNumberedToken is true.
   *
   * <p>This method is designed to be called after UDAF pushdown returns from OpenSearch. The UDAF
   * returns patterns with wildcards, and this method transforms them to numbered tokens and
   * extracts token values from sample logs.
   *
   * @param pattern The pattern string with wildcards (e.g., &lt;*&gt;, &lt;*IP*&gt;)
   * @param sampleLogs List of sample log messages
   * @param showNumberedToken Whether to transform to numbered tokens and extract token values
   * @return Map containing pattern (possibly transformed) and tokens (if showNumberedToken is true)
   */
  public static Object evalAggSamples(
      @Parameter(name = "pattern") String pattern,
      @Parameter(name = "sample_logs") List<String> sampleLogs,
      @Parameter(name = "showNumberedToken") Boolean showNumberedToken) {
    if (Strings.isBlank(pattern)) {
      return EMPTY_RESULT;
    }

    Map<String, List<String>> tokensMap = new HashMap<>();
    String outputPattern = pattern; // Default: return pattern as-is (with wildcards)

    if (Boolean.TRUE.equals(showNumberedToken)) {
      // Parse pattern with wildcard format (<*>, <*IP*>, etc.)
      ParseResult parseResult = PatternUtils.parsePattern(pattern, PatternUtils.WILDCARD_PATTERN);

      // Transform pattern from wildcards to numbered tokens (<token1>, <token2>, etc.)
      outputPattern = parseResult.toTokenOrderString(PatternUtils.TOKEN_PREFIX);

      // Extract token values from sample logs
      for (String sampleLog : sampleLogs) {
        PatternUtils.extractVariables(parseResult, sampleLog, tokensMap, PatternUtils.TOKEN_PREFIX);
      }
    }

    return ImmutableMap.of(PatternUtils.PATTERN, outputPattern, PatternUtils.TOKENS, tokensMap);
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
      } else if (preprocessedToken.startsWith("<*") && candidateToken.startsWith("<*")) {
        // Both are wildcards (potentially different types like <*> vs <*IP*>)
        score += 1;
      }
    }
    return (float) score / tokens.size();
  }
}
