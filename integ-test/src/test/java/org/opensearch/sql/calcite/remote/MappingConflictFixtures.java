/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.util.List;

/**
 * The data the OTel data stream ITs run over, meaning the field types the drifts cross, the PPL
 * commands, and the 500s pinned to their issues. {@link OtelDataStreamConflictTestCase} holds the
 * flow, so a fix that clears a 500 edits only this file.
 */
final class MappingConflictFixtures {

  /** The field types the drifts cross, each against the others and against the field absent. */
  static final List<String> TYPES =
      List.of("text", "keyword", "byte", "integer", "long", "date", "object");

  /**
   * A PPL command template, holding {@code {I}} for the source, {@code {F}} for the field under
   * test and {@code {K}} for a control field.
   */
  record Command(String name, String template) {
    String query(String source, String field, String control) {
      return template.replace("{I}", source).replace("{F}", field).replace("{K}", control);
    }
  }

  /**
   * Representative commands, one per way PPL reads, filters, aggregates, orders or windows the
   * field, covering each command family the mapping conflict issues in #5829 report as a 500.
   */
  static final List<Command> REPRESENTATIVE =
      List.of(
          new Command("where_isnotnull", "source={I} | where isnotnull({F}) | fields {K}"),
          new Command("fields_keep", "source={I} | fields {F}"),
          new Command("eval_cast_string", "source={I} | eval x = cast({F} as string) | fields x"),
          new Command("stats_count_by", "source={I} | stats count() by {F}"),
          new Command("stats_max", "source={I} | stats max({F})"),
          new Command("stats_dc", "source={I} | stats dc({F})"),
          new Command("stats", "source={I} | stats count()"),
          new Command("eventstats_by", "source={I} | eventstats count() by {F}"),
          new Command(
              "streamstats_count_by", "source={I} | streamstats count() by {F} | fields {K}"),
          new Command("timechart_by", "source={I} | timechart span=1d count() by {F}"),
          new Command("chart_by", "source={I} | chart count() over {K} by {F}"),
          new Command("sort", "source={I} | sort {F}"),
          new Command("reverse_after_sort", "source={I} | sort {F} | reverse | fields {K}"),
          new Command("head_after_sort", "source={I} | sort {F} | head 1 | fields {K}"),
          new Command("dedup", "source={I} | dedup {F}"),
          new Command("top", "source={I} | top {F}"),
          new Command("rare", "source={I} | rare {F}"),
          new Command("rex_named_group", "source={I} | rex field={F} '(?<rr>.+)' | fields rr"),
          new Command(
              "join_self",
              "source={I} | head 100 | inner join left = l right = r on l.{F} = r.{F} [ source={I}"
                  + " | head 100 ] | head 10"));

  /**
   * Every listed command over every listed pair returns a 500 under {@code winner}, the PPL type
   * the merged schema reports, or under every winner the pair shows when {@code winner} is null. A
   * fix narrows its pin to the cells that still fail, splitting the pin if they no longer form one
   * set of commands over one set of pairs, and the run fails until it does.
   */
  record Pin(String issue, List<String> commands, List<String> pairs, String winner) {
    Pin {
      if (commands.isEmpty() || pairs.isEmpty()) {
        throw new IllegalArgumentException("The " + issue + " pin needs commands and pairs");
      }
    }

    boolean covers(String pair, String cellWinner, String command) {
      return pairs.contains(pair)
          && commands.contains(command)
          && (winner == null || winner.equals(cellWinner));
    }
  }

  /**
   * The cells that return a 500 today, grouped by the issue that tracks them. Logs and traces
   * return the same ones, since the drift lands on the same fields. Each pin lists its own commands
   * and pairs, so a fix edits only its own entry.
   */
  static final List<Pin> KNOWN_500S =
      List.of(
          // #5811 Composite aggregation pushdown does not support a field whose type differs across
          // indices
          new Pin(
              "#5811",
              List.of("stats_count_by", "dedup", "top", "rare", "chart_by", "timechart_by"),
              List.of("keyword/byte", "keyword/integer", "keyword/long"),
              null),
          // #5811 Composite aggregation pushdown does not support a field whose type differs across
          // indices
          new Pin(
              "#5811",
              List.of("stats_count_by", "dedup", "top", "rare", "chart_by", "timechart_by"),
              List.of("keyword/date"),
              "string"),
          // #5825 sort returns 500 when a field is keyword or ip in one index and a number, date or
          // boolean in another
          new Pin(
              "#5825",
              List.of("sort", "reverse_after_sort", "head_after_sort", "join_self"),
              List.of("keyword/byte", "keyword/integer", "keyword/long", "keyword/date"),
              null),
          // #5835 stats max and stats min return 500 when a field is keyword or ip and boolean or a
          // number in another
          new Pin(
              "#5835",
              List.of("stats_max"),
              List.of("keyword/byte", "keyword/integer", "keyword/long", "keyword/date"),
              "string"),
          // #5836 sort, stats max, and stats min on an object or nested field return 500
          new Pin(
              "#5836",
              List.of("sort", "reverse_after_sort", "head_after_sort", "join_self", "stats_max"),
              List.of("text/object", "object/absent", "absent/object"),
              "struct"));

  private MappingConflictFixtures() {}
}
