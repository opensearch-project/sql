/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.utils;

import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DecimalLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RareCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StringLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TopCommandContext;
import static org.opensearch.sql.utils.MLCommonsConstants.ANOMALY_RATE;
import static org.opensearch.sql.utils.MLCommonsConstants.ANOMALY_SCORE_THRESHOLD;
import static org.opensearch.sql.utils.MLCommonsConstants.CENTROIDS;
import static org.opensearch.sql.utils.MLCommonsConstants.DATE_FORMAT;
import static org.opensearch.sql.utils.MLCommonsConstants.DISTANCE_TYPE;
import static org.opensearch.sql.utils.MLCommonsConstants.ITERATIONS;
import static org.opensearch.sql.utils.MLCommonsConstants.NUMBER_OF_TREES;
import static org.opensearch.sql.utils.MLCommonsConstants.OUTPUT_AFTER;
import static org.opensearch.sql.utils.MLCommonsConstants.SAMPLE_SIZE;
import static org.opensearch.sql.utils.MLCommonsConstants.SHINGLE_SIZE;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_DECAY;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_FIELD;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_ZONE;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAINING_DATA_SIZE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.antlr.v4.runtime.ParserRuleContext;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.AdCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.AdParameterContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.KmeansCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.KmeansParameterContext;

/**
 * Util class to get all arguments as a list from the PPL command.
 */
public class ArgumentFactory {

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx FieldsCommandContext instance
   * @return the list of arguments fetched from the fields command
   */
  public static List<Argument> getArgumentList(FieldsCommandContext ctx) {
    return Collections.singletonList(
        ctx.MINUS() != null
            ? new Argument("exclude", new Literal(true, DataType.BOOLEAN))
            : new Argument("exclude", new Literal(false, DataType.BOOLEAN))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx StatsCommandContext instance
   * @return the list of arguments fetched from the stats command
   */
  public static List<Argument> getArgumentList(StatsCommandContext ctx) {
    return Arrays.asList(
        ctx.partitions != null
            ? new Argument("partitions", getArgumentValue(ctx.partitions))
            : new Argument("partitions", new Literal(1, DataType.INTEGER)),
        ctx.allnum != null
            ? new Argument("allnum", getArgumentValue(ctx.allnum))
            : new Argument("allnum", new Literal(false, DataType.BOOLEAN)),
        ctx.delim != null
            ? new Argument("delim", getArgumentValue(ctx.delim))
            : new Argument("delim", new Literal(" ", DataType.STRING)),
        ctx.dedupsplit != null
            ? new Argument("dedupsplit", getArgumentValue(ctx.dedupsplit))
            : new Argument("dedupsplit", new Literal(false, DataType.BOOLEAN))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx DedupCommandContext instance
   * @return the list of arguments fetched from the dedup command
   */
  public static List<Argument> getArgumentList(DedupCommandContext ctx) {
    return Arrays.asList(
        ctx.number != null
            ? new Argument("number", getArgumentValue(ctx.number))
            : new Argument("number", new Literal(1, DataType.INTEGER)),
        ctx.keepempty != null
            ? new Argument("keepempty", getArgumentValue(ctx.keepempty))
            : new Argument("keepempty", new Literal(false, DataType.BOOLEAN)),
        ctx.consecutive != null
            ? new Argument("consecutive", getArgumentValue(ctx.consecutive))
            : new Argument("consecutive", new Literal(false, DataType.BOOLEAN))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx SortFieldContext instance
   * @return the list of arguments fetched from the sort field in sort command
   */
  public static List<Argument> getArgumentList(SortFieldContext ctx) {
    return Arrays.asList(
        ctx.MINUS() != null
            ? new Argument("asc", new Literal(false, DataType.BOOLEAN))
            : new Argument("asc", new Literal(true, DataType.BOOLEAN)),
        ctx.sortFieldExpression().AUTO() != null
            ? new Argument("type", new Literal("auto", DataType.STRING))
            : ctx.sortFieldExpression().IP() != null
            ? new Argument("type", new Literal("ip", DataType.STRING))
            : ctx.sortFieldExpression().NUM() != null
            ? new Argument("type", new Literal("num", DataType.STRING))
            : ctx.sortFieldExpression().STR() != null
            ? new Argument("type", new Literal("str", DataType.STRING))
            : new Argument("type", new Literal(null, DataType.NULL))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx TopCommandContext instance
   * @return the list of arguments fetched from the top command
   */
  public static List<Argument> getArgumentList(TopCommandContext ctx) {
    return Collections.singletonList(
        ctx.number != null
            ? new Argument("noOfResults", getArgumentValue(ctx.number))
            : new Argument("noOfResults", new Literal(10, DataType.INTEGER))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx RareCommandContext instance
   * @return the list of argument with default number of results for the rare command
   */
  public static List<Argument> getArgumentList(RareCommandContext ctx) {
    return Collections
        .singletonList(new Argument("noOfResults", new Literal(10, DataType.INTEGER)));
  }

  /**
   * Get a map of {@link Argument}.
   *
   * @param ctx KmeansCommandContext instance
   * @return a map of arguments fetched from the kmeans command
   */
  public static Map<String, Literal> getArgumentMap(KmeansCommandContext ctx) {
    List<OpenSearchPPLParser.KmeansParameterContext> kmeansParameters = ctx.kmeansParameter();
    Literal centroids = null;
    Literal iterations = null;
    Literal distanceType = null;

    for (KmeansParameterContext p : kmeansParameters) {
      if (p.centroids != null) {
        centroids = getArgumentValue(p.centroids);
      }
      if (p.iterations != null) {
        iterations = getArgumentValue(p.iterations);
      }
      if (p.distance_type != null) {
        distanceType = getArgumentValue(p.distance_type);
      }
    }

    Map<String, Literal> params = new HashMap<>();
    params.put(CENTROIDS, centroids != null
            ? centroids : new Literal(null, DataType.INTEGER));
    params.put(ITERATIONS, iterations != null
            ? iterations : new Literal(null, DataType.INTEGER));
    params.put(DISTANCE_TYPE, distanceType != null
            ? distanceType : new Literal(null, DataType.STRING));
    return params;
  }

  /**
   * Get a map of {@link Argument}.
   *
   * @param ctx ADCommandContext instance
   * @return a map of arguments fetched from the AD command
   */
  public static Map<String, Literal> getArgumentMap(AdCommandContext ctx) {
    List<AdParameterContext> adParameters = ctx.adParameter();
    Literal numberOfTrees = null;
    Literal shingleSize = null;
    Literal sampleSize = null;
    Literal outputAfter = null;
    Literal timeDecay = null;
    Literal anomalyRate = null;
    Literal timeField = null;
    Literal dateFormat = null;
    Literal timeZone = null;
    Literal trainingDataSize = null;
    Literal anomalyScoreThreshold = null;

    for (AdParameterContext p : adParameters) {
      if (p.number_of_trees != null) {
        numberOfTrees = getArgumentValue(p.number_of_trees);
      }
      if (p.shingle_size != null) {
        shingleSize = getArgumentValue(p.shingle_size);
      }
      if (p.sample_size != null) {
        sampleSize = getArgumentValue(p.sample_size);
      }
      if (p.output_after != null) {
        outputAfter = getArgumentValue(p.output_after);
      }
      if (p.time_decay != null) {
        timeDecay = getArgumentValue(p.time_decay);
      }
      if (p.anomaly_rate != null) {
        anomalyRate = getArgumentValue(p.anomaly_rate);
      }
      if (p.time_field != null) {
        timeField = getArgumentValue(p.time_field);
      }
      if (p.date_format != null) {
        dateFormat = getArgumentValue(p.date_format);
      }
      if (p.time_zone != null) {
        timeZone = getArgumentValue(p.time_zone);
      }
      if (p.training_data_size != null) {
        trainingDataSize = getArgumentValue(p.training_data_size);
      }
      if (p.anomaly_score_threshold != null) {
        anomalyScoreThreshold = getArgumentValue(p.anomaly_score_threshold);
      }
    }

    if (timeField != null && dateFormat == null) {
      dateFormat = new Literal("yyyy-MM-dd HH:mm:ss", DataType.STRING);
    }

    HashMap<String, Literal> params = new HashMap<>();
    params.put(NUMBER_OF_TREES, numberOfTrees != null
            ? numberOfTrees : new Literal(null, DataType.INTEGER));
    params.put(SHINGLE_SIZE, shingleSize != null
            ? shingleSize : new Literal(null, DataType.INTEGER));
    params.put(SAMPLE_SIZE, sampleSize != null
            ? sampleSize : new Literal(null, DataType.INTEGER));
    params.put(OUTPUT_AFTER, outputAfter != null
            ? outputAfter : new Literal(null, DataType.INTEGER));
    params.put(TIME_DECAY, timeDecay != null
            ? timeDecay : new Literal(null, DataType.DOUBLE));
    params.put(ANOMALY_RATE, anomalyRate != null
            ? anomalyRate : new Literal(null, DataType.DOUBLE));
    params.put(TIME_FIELD, timeField != null
            ? timeField : new Literal(null, DataType.STRING));
    params.put(DATE_FORMAT, dateFormat != null
            ? dateFormat : new Literal(null, DataType.STRING));
    params.put(TIME_ZONE, timeZone != null
            ? timeZone : new Literal(null, DataType.STRING));
    params.put(TRAINING_DATA_SIZE, trainingDataSize != null
            ? trainingDataSize : new Literal(null, DataType.INTEGER));
    params.put(ANOMALY_SCORE_THRESHOLD, anomalyScoreThreshold != null
            ? anomalyScoreThreshold : new Literal(null, DataType.DOUBLE));

    return params;
  }

  private static Literal getArgumentValue(ParserRuleContext ctx) {
    return ctx instanceof IntegerLiteralContext
        ? new Literal(Integer.parseInt(ctx.getText()), DataType.INTEGER)
        : ctx instanceof BooleanLiteralContext
        ? new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN)
        : ctx instanceof DecimalLiteralContext
        ? new Literal(Double.valueOf(ctx.getText()), DataType.DOUBLE)
        : new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
  }

}
