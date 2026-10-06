/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.fail;
import static org.opensearch.sql.calcite.remote.MappingConflictFixtures.KNOWN_500S;
import static org.opensearch.sql.calcite.remote.MappingConflictFixtures.REPRESENTATIVE;
import static org.opensearch.sql.calcite.remote.MappingConflictFixtures.TYPES;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.calcite.remote.MappingConflictFixtures.Command;
import org.opensearch.sql.calcite.remote.MappingConflictFixtures.Known500;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.OtelDataStream;
import org.opensearch.sql.util.OtelDataStream.Drift;

/**
 * Runs PPL commands over an OTel data stream whose second backing index changes the type of one
 * {@code attributes.types} field, removes it, or adds one, through the stream name and a wildcard.
 * That change is the drift. A pair names the two types the field takes, with {@code absent} for the
 * index that lacks it, and the winner is the type the merged schema reports for the field. Every
 * pair of {@link MappingConflictFixtures#TYPES} runs twice, once with each type in 000001. One node
 * reads the backing indices in a fixed order, so the two scenarios of a pair feed the merge both
 * orders a two-index stream can produce and cover every winner production can return. A 500 fails
 * the run unless {@link MappingConflictFixtures#KNOWN_500S} lists it as a known 500 tied to an
 * issue, and a known 500 that stops returning 500 fails too. A 200 or 4xx outside {@code
 * KNOWN_500S} passes, since this checks only that schema evolution never returns a 500, but a
 * command that never returns 200 fails, since it tests nothing.
 */
public abstract class OtelDataStreamConflictTestCase extends PPLIntegTestCase {

  private static final int BASE_DOCS = 5;

  /** Creates the stream with its base documents in 000001. */
  protected abstract OtelDataStream create(String name) throws IOException;

  /** A keyword field no drift touches, for {@code {K}}. */
  protected abstract String stableField();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  /**
   * Runs every drift under {@code name} and fails on a 500 missing from {@code KNOWN_500S}, a stale
   * {@code KNOWN_500S} entry, or a command that never returns 200.
   */
  protected void runDrifts(String name) throws IOException {
    requireOneNode();
    Observations observed = observe(name, Scenario.allDrifts(TYPES));
    logSummary(observed);
    List<String> failures = new ArrayList<>();
    failures.addAll(observed.unreadableWinners());
    failures.addAll(observed.commandsThatNeverReturn200());
    failures.addAll(observed.missingKnown500s());
    failures.addAll(observed.staleKnown500s());
    if (!failures.isEmpty()) {
      Set<String> distinct = new TreeSet<>(failures);
      fail(distinct.size() + " failures\n" + String.join("\n", distinct));
    }
  }

  /**
   * The merged schema keeps the type of the backing index it iterates last, in an order set by the
   * index names and a salt each JVM draws at start. Every scenario recreates the same two names, so
   * one node gives them all one order, and running a pair as a/b and then b/a lets each type win
   * once. Each node draws its own salt and requests spread across nodes, so several nodes break it.
   */
  private void requireOneNode() throws IOException {
    JSONArray nodes = new JSONArray(executeRequest(new Request("GET", "/_cat/nodes?format=json")));
    if (nodes.length() != 1) {
      throw new IllegalStateException(
          "The scenarios need one node to hold the merge order fixed, the cluster has "
              + nodes.length());
    }
  }

  /**
   * One rollover, meaning the pair its queries report under, the field they read, and the drift
   * that 000002 applies.
   */
  private record Scenario(String pair, String field, DriftFactory drift) {

    /**
     * Every pair of {@code types} twice, once with each type in 000001, then each type removed and
     * each type added, so 56 scenarios for 7 types.
     */
    static List<Scenario> allDrifts(List<String> types) {
      List<Scenario> scenarios = new ArrayList<>();
      for (int i = 0; i < types.size(); i++) {
        for (int j = i + 1; j < types.size(); j++) {
          String a = types.get(i);
          String b = types.get(j);
          String pair = a + "/" + b;
          scenarios.add(new Scenario(pair, "attributes.types." + a, retype(a, b)));
          scenarios.add(new Scenario(pair, "attributes.types." + b, retype(b, a)));
        }
      }
      for (String type : types) {
        String field = "attributes.types." + type;
        scenarios.add(new Scenario(type + "/absent", field, n -> Drift.remove(field, BASE_DOCS)));
        String added = "attributes.types.added_" + type;
        scenarios.add(
            new Scenario(
                "absent/" + type,
                added,
                n -> Drift.add(added, type, baseValues(n, "types." + type).toArray())));
      }
      return scenarios;
    }
  }

  /** Builds a drift once the stream exists, since its values come from the base documents. */
  @FunctionalInterface
  private interface DriftFactory {
    Drift build(String name) throws IOException;
  }

  /** Changes {@code from}'s field to {@code to}, filled with {@code to}'s base values. */
  private static DriftFactory retype(String from, String to) {
    return n ->
        Drift.changeType("attributes.types." + from, to, baseValues(n, "types." + to).toArray());
  }

  private Observations observe(String name, List<Scenario> scenarios) throws IOException {
    List<Observation> all = new ArrayList<>();
    for (Scenario scenario : scenarios) {
      all.addAll(observe(name, scenario));
    }
    return new Observations(all);
  }

  /**
   * Creates the stream, rolls it over to the scenario's drift, and runs every command through the
   * stream name and the wildcard, each under the winner its merged schema reports.
   */
  private List<Observation> observe(String name, Scenario scenario) throws IOException {
    OtelDataStream stream = create(name);
    List<Observation> observations = new ArrayList<>();
    try {
      String field = "`" + scenario.field() + "`";
      stream.rollover(scenario.drift().build(name));
      for (String source : List.of(name, name + "*")) {
        String resolved = winner(source, field);
        logger.info("pair {}, field {} via {} reads {}", scenario.pair(), field, source, resolved);
        for (Command command : REPRESENTATIVE) {
          String query = command.query(source, field, stableField());
          Response response = run(query);
          observations.add(
              new Observation(
                  scenario.pair(),
                  resolved,
                  command.name(),
                  source,
                  query,
                  response.status(),
                  response.error()));
        }
      }
    } catch (IOException | RuntimeException e) {
      try {
        stream.delete();
      } catch (IOException | RuntimeException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
    stream.delete();
    return observations;
  }

  private void logSummary(Observations observed) {
    Set<String> keys = new TreeSet<>();
    for (Observation o : observed.all()) {
      if (o.status() >= 500) {
        keys.add(o.key());
        logger.info("500 {} | {} | {}", o.key(), o.source(), o.error());
      }
    }
    logger.info(
        "{} queries, {} return 500 across {} pair, winner and command keys",
        observed.all().size(),
        observed.all().stream().filter(o -> o.status() >= 500).count(),
        keys.size());
  }

  /** Every observation of a run, with the checks that turn them into failure lines. */
  private record Observations(List<Observation> all) {

    /**
     * A winner probe that failed. A 4xx on a type pair passes, since a fix may reject the conflict,
     * but not on a pair with an absent side, where nothing conflicts.
     */
    List<String> unreadableWinners() {
      return all.stream()
          .filter(
              o ->
                  o.winner().startsWith("unread-")
                      && (o.pair().contains("absent") || !o.winner().startsWith("unread-4")))
          .map(o -> o.pair() + " via " + o.source() + " could not read its winner, " + o.winner())
          .toList();
    }

    List<String> commandsThatNeverReturn200() {
      return REPRESENTATIVE.stream()
          .map(Command::name)
          .filter(n -> all.stream().noneMatch(o -> o.command().equals(n) && o.status() == 200))
          .map(n -> n + " never returned 200, so it tests nothing")
          .toList();
    }

    List<String> missingKnown500s() {
      return all.stream()
          .filter(o -> o.status() >= 500 && KNOWN_500S.stream().noneMatch(o::isCoveredBy))
          .map(
              o ->
                  "500 missing from KNOWN_500S, "
                      + o.label()
                      + "\n  query: "
                      + o.query()
                      + "\n  error: "
                      + o.error().substring(0, Math.min(200, o.error().length())))
          .toList();
    }

    /**
     * A {@code KNOWN_500S} entry that lists a command or pair the run never reaches, or covers a
     * query that no longer returns 500.
     */
    List<String> staleKnown500s() {
      Set<String> pairs = all.stream().map(Observation::pair).collect(Collectors.toSet());
      List<String> failures = new ArrayList<>();
      for (Known500 known : KNOWN_500S) {
        known.commands().stream()
            .filter(c -> REPRESENTATIVE.stream().noneMatch(command -> command.name().equals(c)))
            .forEach(
                c ->
                    failures.add(
                        "the " + known.issue() + " known 500 lists " + c + ", which never runs"));
        List<Observation> covered = all.stream().filter(o -> o.isCoveredBy(known)).toList();
        known.pairs().stream()
            .filter(p -> covered.stream().noneMatch(o -> o.pair().equals(p)))
            .forEach(
                p ->
                    failures.add(
                        "the "
                            + known.issue()
                            + " known 500 lists "
                            + p
                            + (pairs.contains(p)
                                ? ", which yields no query under its winner and source, so fix or"
                                    + " remove it"
                                : ", which never runs, so check its spelling and TYPES order")));
        covered.stream()
            .filter(o -> o.status() < 500)
            .forEach(
                o ->
                    failures.add(
                        "listed under "
                            + known.issue()
                            + " in KNOWN_500S but no longer returns 500, "
                            + o.label()
                            + ", narrow that entry to the queries that still fail\n  query: "
                            + o.query()));
      }
      return failures;
    }
  }

  /** The PPL type the merged schema gives the field, or the status when it cannot be read. */
  private String winner(String source, String field) throws IOException {
    Response response = run("source=" + source + " | fields " + field + " | head 1");
    return response.status() == 200 && !response.type().isEmpty()
        ? response.type()
        : "unread-" + response.status();
  }

  /** Each base document's value for a flattened {@code attributes} key. */
  private static List<Object> baseValues(String name, String key) throws IOException {
    JSONObject body =
        new JSONObject(
            getResponseBody(
                client().performRequest(new Request("GET", "/" + name + "/_search?size=100"))));
    JSONArray hits = body.getJSONObject("hits").getJSONArray("hits");
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < hits.length(); i++) {
      values.add(
          hits.getJSONObject(i).getJSONObject("_source").getJSONObject("attributes").get(key));
    }
    if (values.size() != BASE_DOCS) {
      throw new IllegalStateException(
          name + " holds " + values.size() + " base documents, expected " + BASE_DOCS);
    }
    return values;
  }

  private Response run(String query) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(new JSONObject().put("query", query).toString());
    try {
      JSONObject body = new JSONObject(getResponseBody(client().performRequest(request)));
      JSONArray schema = body.optJSONArray("schema");
      return new Response(
          200,
          schema == null || schema.isEmpty() ? "" : schema.getJSONObject(0).optString("type"),
          "");
    } catch (ResponseException e) {
      String error = "";
      try {
        JSONObject json = new JSONObject(getResponseBody(e.getResponse())).optJSONObject("error");
        if (json != null) {
          String details = json.optString("details");
          error =
              json.optString("type")
                  + ": "
                  + (details.isEmpty() ? json.optString("reason") : details);
        }
      } catch (IOException | RuntimeException ignored) {
        // keep the status alone
      }
      error = error.replace('\n', ' ');
      return new Response(e.getResponse().getStatusLine().getStatusCode(), "", error);
    }
  }

  private record Response(int status, String type, String error) {}

  /** The outcome of one query, meaning one command over one source under one winner. */
  private record Observation(
      String pair,
      String winner,
      String command,
      String source,
      String query,
      int status,
      String error) {

    /** The pair, winner and command, the unit the run counts 500s by. */
    String key() {
      return pair + " " + winner + " " + command;
    }

    /** The observation as a failure line reads it. */
    String label() {
      return "pair " + pair + ", winner " + winner + ", command " + command;
    }

    boolean isCoveredBy(Known500 known) {
      return known.covers(pair, winner, command, source);
    }
  }
}
