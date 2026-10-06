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
 * Every pair of {@link MappingConflictFixtures#TYPES} runs twice, once with each type in 000001.
 * One node reads the backing indices in a fixed order, so the two passes feed the merge both orders
 * a two-index stream can produce and cover every winner production can return. A 500 fails the run
 * unless {@link MappingConflictFixtures#KNOWN_500S} lists it as a known 500 tied to an issue, and a
 * known 500 that stops returning 500 fails too. A 200 or 4xx outside {@code KNOWN_500S} passes,
 * since this checks only that schema evolution never returns a 500, but a command that never
 * returns 200 fails, since it tests nothing.
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
    JSONArray nodes = new JSONArray(executeRequest(new Request("GET", "/_cat/nodes?format=json")));
    if (nodes.length() != 1) {
      throw new IllegalStateException(
          "The swap needs one node to fix the merge order, the cluster has " + nodes.length());
    }
    List<Command> commands = REPRESENTATIVE;
    Set<String> names = commands.stream().map(Command::name).collect(Collectors.toSet());
    List<Observation> observations = new ArrayList<>();
    List<String> failures = new ArrayList<>();
    for (int i = 0; i < TYPES.size(); i++) {
      for (int j = i + 1; j < TYPES.size(); j++) {
        String a = TYPES.get(i);
        String b = TYPES.get(j);
        String pair = a + "/" + b;
        observations.addAll(observe(name, pair, a, retype(a, b), commands));
        observations.addAll(observe(name, pair, b, retype(b, a), commands));
      }
    }
    for (String type : TYPES) {
      String field = "attributes.types." + type;
      observations.addAll(
          observe(name, type + "/absent", type, n -> Drift.remove(field, BASE_DOCS), commands));
      String added = "added_" + type;
      observations.addAll(
          observe(
              name,
              "absent/" + type,
              added,
              n ->
                  Drift.add(
                      "attributes.types." + added, type, baseValues(n, "types." + type).toArray()),
              commands));
    }
    observations.stream()
        .filter(
            o ->
                o.winner().startsWith("unread-")
                    && (o.pair().contains("absent") || !o.winner().startsWith("unread-4")))
        .forEach(
            o ->
                failures.add(
                    o.pair() + " via " + o.source() + " could not read its winner, " + o.winner()));
    names.stream()
        .filter(
            n -> observations.stream().noneMatch(o -> o.command().equals(n) && o.status() == 200))
        .forEach(n -> failures.add(n + " never returned 200, so it tests nothing"));
    Set<String> serverErrors = new TreeSet<>();
    for (Observation observation : observations) {
      if (observation.status() >= 500) {
        serverErrors.add(observation.key());
        logger.info(
            "500 {} | {} | {}", observation.key(), observation.source(), observation.error());
        if (KNOWN_500S.stream()
            .noneMatch(
                k ->
                    k.covers(
                        observation.pair(),
                        observation.winner(),
                        observation.command(),
                        observation.source()))) {
          failures.add(
              "500 missing from KNOWN_500S, "
                  + observation.label()
                  + "\n  query: "
                  + observation.query()
                  + "\n  error: "
                  + observation.error().substring(0, Math.min(200, observation.error().length())));
        }
      }
    }
    logger.info(
        "{} queries, {} return 500 across {} pair, winner and command keys",
        observations.size(),
        observations.stream().filter(o -> o.status() >= 500).count(),
        serverErrors.size());
    Set<String> pairs = observations.stream().map(Observation::pair).collect(Collectors.toSet());
    for (Known500 known : KNOWN_500S) {
      known.commands().stream()
          .filter(c -> !names.contains(c))
          .forEach(
              c ->
                  failures.add(
                      "the " + known.issue() + " known 500 lists " + c + ", which never runs"));
      List<Observation> covered =
          observations.stream()
              .filter(o -> known.covers(o.pair(), o.winner(), o.command(), o.source()))
              .toList();
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
    if (!failures.isEmpty()) {
      Set<String> distinct = new TreeSet<>(failures);
      fail(distinct.size() + " failures\n" + String.join("\n", distinct));
    }
  }

  /** Builds a drift once the stream exists, since its values come from the base documents. */
  @FunctionalInterface
  private interface DriftFactory {
    Drift drift(String name) throws IOException;
  }

  /** Changes {@code from}'s field to {@code to}, filled with {@code to}'s base values. */
  private DriftFactory retype(String from, String to) {
    return n ->
        Drift.changeType("attributes.types." + from, to, baseValues(n, "types." + to).toArray());
  }

  /**
   * Rolls the stream over to the drift and runs every command through the stream name and the
   * wildcard, each under the winner its merged schema reports.
   */
  private List<Observation> observe(
      String name, String pair, String type, DriftFactory factory, List<Command> commands)
      throws IOException {
    OtelDataStream stream = create(name);
    List<Observation> observations = new ArrayList<>();
    try {
      String field = "`attributes.types." + type + "`";
      stream.rollover(factory.drift(name));
      for (String source : List.of(name, name + "*")) {
        String resolved = winner(source, field);
        logger.info("pair {}, field {} via {} reads {}", pair, field, source, resolved);
        for (Command command : commands) {
          String query = command.query(source, field, stableField());
          Response response = run(query);
          observations.add(
              new Observation(
                  pair,
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

  /** The PPL type the merged schema gives the field, or the status when it cannot be read. */
  private String winner(String source, String field) throws IOException {
    Response response = run("source=" + source + " | fields " + field + " | head 1");
    return response.status() == 200 && !response.type().isEmpty()
        ? response.type()
        : "unread-" + response.status();
  }

  /** Each base document's value for a flattened {@code attributes} key. */
  private List<Object> baseValues(String name, String key) throws IOException {
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

    String key() {
      return pair + " " + winner + " " + command;
    }

    /** The observation as a failure line reads it. */
    String label() {
      return "pair " + pair + ", winner " + winner + ", command " + command;
    }
  }
}
