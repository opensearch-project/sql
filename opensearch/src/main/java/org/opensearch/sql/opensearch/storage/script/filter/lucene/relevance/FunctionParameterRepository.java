/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.support.QueryParsers;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.sql.data.model.ExprValue;

@UtilityClass
public class FunctionParameterRepository {

  public static final Map<String, RelevanceQuery.QueryBuilderStep<MatchBoolPrefixQueryBuilder>>
      MatchBoolPrefixQueryBuildActions =
          ImmutableMap
              .<String, RelevanceQuery.QueryBuilderStep<MatchBoolPrefixQueryBuilder>>builder()
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put("fuzziness", (b, v) -> b.fuzziness(convertFuzziness(v)))
              .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(checkRewrite(v, "fuzzy_rewrite")))
              .put(
                  "fuzzy_transpositions",
                  (b, v) -> b.fuzzyTranspositions(convertBoolValue(v, "fuzzy_transpositions")))
              .put(
                  "max_expansions", (b, v) -> b.maxExpansions(convertIntValue(v, "max_expansions")))
              .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
              .put("operator", (b, v) -> b.operator(convertOperator(v, "operator")))
              .put("prefix_length", (b, v) -> b.prefixLength(convertIntValue(v, "prefix_length")))
              .build();

  public static final Map<String, RelevanceQuery.QueryBuilderStep<MatchPhrasePrefixQueryBuilder>>
      MatchPhrasePrefixQueryBuildActions =
          ImmutableMap
              .<String, RelevanceQuery.QueryBuilderStep<MatchPhrasePrefixQueryBuilder>>builder()
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put(
                  "max_expansions", (b, v) -> b.maxExpansions(convertIntValue(v, "max_expansions")))
              .put("slop", (b, v) -> b.slop(convertIntValue(v, "slop")))
              .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(convertZeroTermsQuery(v)))
              .build();

  public static final Map<String, RelevanceQuery.QueryBuilderStep<MatchPhraseQueryBuilder>>
      MatchPhraseQueryBuildActions =
          ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<MatchPhraseQueryBuilder>>builder()
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put("slop", (b, v) -> b.slop(convertIntValue(v, "slop")))
              .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(convertZeroTermsQuery(v)))
              .build();

  public static final Map<String, RelevanceQuery.QueryBuilderStep<MatchQueryBuilder>>
      MatchQueryBuildActions =
          ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<MatchQueryBuilder>>builder()
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put(
                  "auto_generate_synonyms_phrase_query",
                  (b, v) ->
                      b.autoGenerateSynonymsPhraseQuery(
                          convertBoolValue(v, "auto_generate_synonyms_phrase_query")))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put("fuzziness", (b, v) -> b.fuzziness(convertFuzziness(v)))
              .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(checkRewrite(v, "fuzzy_rewrite")))
              .put(
                  "fuzzy_transpositions",
                  (b, v) -> b.fuzzyTranspositions(convertBoolValue(v, "fuzzy_transpositions")))
              .put("lenient", (b, v) -> b.lenient(convertBoolValue(v, "lenient")))
              .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
              .put(
                  "max_expansions", (b, v) -> b.maxExpansions(convertIntValue(v, "max_expansions")))
              .put("operator", (b, v) -> b.operator(convertOperator(v, "operator")))
              .put("prefix_length", (b, v) -> b.prefixLength(convertIntValue(v, "prefix_length")))
              .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(convertZeroTermsQuery(v)))
              .build();

  @SuppressWarnings("deprecation") // cutoffFrequency is deprecated
  public static final Map<String, RelevanceQuery.QueryBuilderStep<MultiMatchQueryBuilder>>
      MultiMatchQueryBuildActions =
          ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<MultiMatchQueryBuilder>>builder()
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put(
                  "auto_generate_synonyms_phrase_query",
                  (b, v) ->
                      b.autoGenerateSynonymsPhraseQuery(
                          convertBoolValue(v, "auto_generate_synonyms_phrase_query")))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put(
                  "cutoff_frequency",
                  (b, v) -> b.cutoffFrequency(convertFloatValue(v, "cutoff_frequency")))
              .put("fuzziness", (b, v) -> b.fuzziness(convertFuzziness(v)))
              .put(
                  "fuzzy_transpositions",
                  (b, v) -> b.fuzzyTranspositions(convertBoolValue(v, "fuzzy_transpositions")))
              .put("lenient", (b, v) -> b.lenient(convertBoolValue(v, "lenient")))
              .put(
                  "max_expansions", (b, v) -> b.maxExpansions(convertIntValue(v, "max_expansions")))
              .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
              .put("operator", (b, v) -> b.operator(convertOperator(v, "operator")))
              .put("prefix_length", (b, v) -> b.prefixLength(convertIntValue(v, "prefix_length")))
              .put("slop", (b, v) -> b.slop(convertIntValue(v, "slop")))
              .put("tie_breaker", (b, v) -> b.tieBreaker(convertFloatValue(v, "tie_breaker")))
              .put("type", (b, v) -> b.type(convertType(v)))
              .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(convertZeroTermsQuery(v)))
              .build();

  public static final Map<String, RelevanceQuery.QueryBuilderStep<QueryStringQueryBuilder>>
      QueryStringQueryBuildActions =
          ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<QueryStringQueryBuilder>>builder()
              .put(
                  "allow_leading_wildcard",
                  (b, v) -> b.allowLeadingWildcard(convertBoolValue(v, "allow_leading_wildcard")))
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put(
                  "analyze_wildcard",
                  (b, v) -> b.analyzeWildcard(convertBoolValue(v, "analyze_wildcard")))
              .put(
                  "auto_generate_synonyms_phrase_query",
                  (b, v) ->
                      b.autoGenerateSynonymsPhraseQuery(
                          convertBoolValue(v, "auto_generate_synonyms_phrase_query")))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put(
                  "default_operator",
                  (b, v) -> b.defaultOperator(convertOperator(v, "default_operator")))
              .put(
                  "enable_position_increments",
                  (b, v) ->
                      b.enablePositionIncrements(convertBoolValue(v, "enable_position_increments")))
              .put("escape", (b, v) -> b.escape(convertBoolValue(v, "escape")))
              .put("fuzziness", (b, v) -> b.fuzziness(convertFuzziness(v)))
              .put(
                  "fuzzy_max_expansions",
                  (b, v) -> b.fuzzyMaxExpansions(convertIntValue(v, "fuzzy_max_expansions")))
              .put(
                  "fuzzy_prefix_length",
                  (b, v) -> b.fuzzyPrefixLength(convertIntValue(v, "fuzzy_prefix_length")))
              .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(checkRewrite(v, "fuzzy_rewrite")))
              .put(
                  "fuzzy_transpositions",
                  (b, v) -> b.fuzzyTranspositions(convertBoolValue(v, "fuzzy_transpositions")))
              .put("lenient", (b, v) -> b.lenient(convertBoolValue(v, "lenient")))
              .put(
                  "max_determinized_states",
                  (b, v) -> b.maxDeterminizedStates(convertIntValue(v, "max_determinized_states")))
              .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
              .put("phrase_slop", (b, v) -> b.phraseSlop(convertIntValue(v, "phrase_slop")))
              .put("quote_analyzer", (b, v) -> b.quoteAnalyzer(v.stringValue()))
              .put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v.stringValue()))
              .put("rewrite", (b, v) -> b.rewrite(checkRewrite(v, "rewrite")))
              .put("tie_breaker", (b, v) -> b.tieBreaker(convertFloatValue(v, "tie_breaker")))
              .put("time_zone", (b, v) -> b.timeZone(checkTimeZone(v)))
              .put("type", (b, v) -> b.type(convertType(v)))
              .build();

  public static final Map<String, RelevanceQuery.QueryBuilderStep<SimpleQueryStringBuilder>>
      SimpleQueryStringQueryBuildActions =
          ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<SimpleQueryStringBuilder>>builder()
              .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
              .put(
                  "analyze_wildcard",
                  (b, v) -> b.analyzeWildcard(convertBoolValue(v, "analyze_wildcard")))
              .put(
                  "auto_generate_synonyms_phrase_query",
                  (b, v) ->
                      b.autoGenerateSynonymsPhraseQuery(
                          convertBoolValue(v, "auto_generate_synonyms_phrase_query")))
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put(
                  "default_operator",
                  (b, v) -> b.defaultOperator(convertOperator(v, "default_operator")))
              .put("flags", (b, v) -> b.flags(convertFlags(v)))
              .put(
                  "fuzzy_max_expansions",
                  (b, v) -> b.fuzzyMaxExpansions(convertIntValue(v, "fuzzy_max_expansions")))
              .put(
                  "fuzzy_prefix_length",
                  (b, v) -> b.fuzzyPrefixLength(convertIntValue(v, "fuzzy_prefix_length")))
              .put(
                  "fuzzy_transpositions",
                  (b, v) -> b.fuzzyTranspositions(convertBoolValue(v, "fuzzy_transpositions")))
              .put("lenient", (b, v) -> b.lenient(convertBoolValue(v, "lenient")))
              .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
              .put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v.stringValue()))
              .build();

  public static final Map<String, RelevanceQuery.QueryBuilderStep<WildcardQueryBuilder>>
      WildcardQueryBuildActions =
          ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<WildcardQueryBuilder>>builder()
              .put("boost", (b, v) -> b.boost(convertFloatValue(v, "boost")))
              .put(
                  "case_insensitive",
                  (b, v) -> b.caseInsensitive(convertBoolValue(v, "case_insensitive")))
              .put("rewrite", (b, v) -> b.rewrite(checkRewrite(v, "rewrite")))
              .build();

  public static final Map<String, String> ArgumentLimitations =
      ImmutableMap.<String, String>builder()
          .put("boost", "Accepts only floating point values greater than 0.")
          .put("tie_breaker", "Accepts only floating point values in range 0 to 1.")
          .put(
              "rewrite",
              "Available values are: constant_score, "
                  + "scoring_boolean, constant_score_boolean, top_terms_X, top_terms_boost_X, "
                  + "top_terms_blended_freqs_X, where X is an integer value.")
          .put(
              "flags",
              String.format(
                  "Available values are: %s and any combinations of these separated by '|'.",
                  Arrays.stream(SimpleQueryStringFlag.class.getEnumConstants())
                      .map(Enum::toString)
                      .collect(Collectors.joining(", "))))
          .put(
              "time_zone",
              "For more information, follow this link: "
                  + "https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-")
          .put(
              "fuzziness",
              "Available values are: " + "'AUTO', 'AUTO:x,y' or z, where x, y, z - integer values.")
          .put(
              "operator",
              String.format(
                  "Available values are: %s.",
                  Arrays.stream(Operator.class.getEnumConstants())
                      .map(Enum::toString)
                      .collect(Collectors.joining(", "))))
          .put(
              "type",
              String.format(
                  "Available values are: %s.",
                  Arrays.stream(MultiMatchQueryBuilder.Type.class.getEnumConstants())
                      .map(Enum::toString)
                      .collect(Collectors.joining(", "))))
          .put(
              "zero_terms_query",
              String.format(
                  "Available values are: %s.",
                  Arrays.stream(MatchQuery.ZeroTermsQuery.class.getEnumConstants())
                      .map(Enum::toString)
                      .collect(Collectors.joining(", "))))
          .put("int", "Accepts only integer values.")
          .put("float", "Accepts only floating point values.")
          .put("bool", "Accepts only boolean values: 'true' or 'false'.")
          .build();

  private static String formatErrorMessage(String name, String value) {
    return formatErrorMessage(name, value, name);
  }

  private static String formatErrorMessage(String name, String value, String limitationName) {
    return String.format(
        "Invalid %s value: '%s'. %s",
        name,
        value,
        ArgumentLimitations.containsKey(name)
            ? ArgumentLimitations.get(name)
            : ArgumentLimitations.getOrDefault(limitationName, ""));
  }

  /**
   * Check whether value is valid for 'rewrite' or 'fuzzy_rewrite'.
   *
   * @param value Value
   * @param name Value name
   * @return Converted
   */
  public static String checkRewrite(ExprValue value, String name) {
    try {
      QueryParsers.parseRewriteMethod(
          value.stringValue().toLowerCase(), null, LoggingDeprecationHandler.INSTANCE);
      return value.stringValue();
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "rewrite"));
    }
  }

  /**
   * Convert ExprValue to Flags.
   *
   * @param value Value
   * @return Array of flags
   */
  public static SimpleQueryStringFlag[] convertFlags(ExprValue value) {
    try {
      return Arrays.stream(value.stringValue().toUpperCase().split("\\|"))
          .map(SimpleQueryStringFlag::valueOf)
          .toArray(SimpleQueryStringFlag[]::new);
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("flags", value.stringValue()), e);
    }
  }

  /**
   * Check whether ExprValue could be converted to timezone object.
   *
   * @param value Value
   * @return Converted to string
   */
  public static String checkTimeZone(ExprValue value) {
    try {
      ZoneId.of(value.stringValue());
      return value.stringValue();
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("time_zone", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to Fuzziness object.
   *
   * @param value Value
   * @return Fuzziness
   */
  public static Fuzziness convertFuzziness(ExprValue value) {
    try {
      return Fuzziness.build(value.stringValue().toUpperCase());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("fuzziness", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to Operator object, could be used for 'operator' and 'default_operator'.
   *
   * @param value Value
   * @param name Value name
   * @return Operator
   */
  public static Operator convertOperator(ExprValue value, String name) {
    try {
      return Operator.fromString(value.stringValue().toUpperCase());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "operator"));
    }
  }

  /**
   * Convert ExprValue to Type object.
   *
   * @param value Value
   * @return Type
   */
  public static MultiMatchQueryBuilder.Type convertType(ExprValue value) {
    try {
      return MultiMatchQueryBuilder.Type.parse(
          value.stringValue().toLowerCase(), LoggingDeprecationHandler.INSTANCE);
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("type", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to ZeroTermsQuery object.
   *
   * @param value Value
   * @return ZeroTermsQuery
   */
  public static MatchQuery.ZeroTermsQuery convertZeroTermsQuery(ExprValue value) {
    try {
      return MatchQuery.ZeroTermsQuery.valueOf(value.stringValue().toUpperCase());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("zero_terms_query", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to int.
   *
   * @param value Value
   * @param name Value name
   * @return int
   */
  public static int convertIntValue(ExprValue value, String name) {
    try {
      return Integer.parseInt(value.stringValue());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "int"), e);
    }
  }

  /**
   * Convert ExprValue to float.
   *
   * @param value Value
   * @param name Value name
   * @return float
   */
  public static float convertFloatValue(ExprValue value, String name) {
    try {
      return Float.parseFloat(value.stringValue());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "float"), e);
    }
  }

  /**
   * Convert ExprValue to bool.
   *
   * @param value Value
   * @param name Value name
   * @return bool
   */
  public static boolean convertBoolValue(ExprValue value, String name) {
    try {
      // Boolean.parseBoolean interprets integers or any other stuff as a valid value
      Boolean res = Boolean.parseBoolean(value.stringValue());
      if (value.stringValue().equalsIgnoreCase(res.toString())) {
        return res;
      } else {
        throw new Exception("Invalid boolean value");
      }
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "bool"), e);
    }
  }
}
