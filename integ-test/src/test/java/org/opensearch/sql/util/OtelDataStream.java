/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import static org.opensearch.sql.legacy.TestUtils.getMappingFile;
import static org.opensearch.sql.legacy.TestUtils.getResourceFilePath;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.loadDataByRestClient;
import static org.opensearch.sql.legacy.TestUtils.performRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;

/**
 * A data stream of real OpenTelemetry Demo logs or spans, mapped by Data Prepper's {@code
 * logs-otel-v1} or {@code otel-v1-apm-span} template. The template declares some fields and dynamic
 * mapping adds the rest, as in the real pipeline. {@link #logs} and {@link #traces} load the base
 * documents into backing index 000001. Each {@link #rollover} starts from 000001's resolved
 * mapping, changes one field, rolls over, and loads a few base documents with that field changed.
 * So every later backing index differs from 000001 in the drifted field only, with its subfields
 * and any parent object an added field needs. The template carries no analytics-engine settings, so
 * callers skip that route.
 */
public class OtelDataStream {

  private static final Signal LOGS =
      new Signal(
          "otel_logs_data_stream_template.json",
          "src/test/resources/otel_logs_data_stream.json",
          List.of("time"));
  private static final Signal TRACES =
      new Signal(
          "otel_traces_data_stream_template.json",
          "src/test/resources/otel_traces_data_stream.json",
          List.of("startTime", "endTime", "events.time", "traceGroupFields.endTime"));
  private static final List<String> OBJECT_TYPES = List.of("object", "nested", "flat_object");

  /** A template, its documents, and the time fields that move with {@code @timestamp}. */
  private record Signal(String templateFile, String dataFile, List<String> timeFields) {}

  private final RestClient client;
  private final String name;
  private final Signal signal;
  private final JSONObject template;
  private final List<JSONObject> baseDocs;
  private JSONObject resolvedMapping;
  private int generation = 1;

  private OtelDataStream(RestClient client, String name, Signal signal) throws IOException {
    this.client = client;
    this.name = name;
    this.signal = signal;
    this.template =
        new JSONObject(
            Objects.requireNonNull(getMappingFile(signal.templateFile()), signal.templateFile()));
    this.template.put("index_patterns", new JSONArray().put(name)).put("priority", 100);
    this.baseDocs = readBaseDocs(signal.dataFile());
  }

  /** Creates a logs data stream and loads the base documents. */
  public static OtelDataStream logs(RestClient client, String name) throws IOException {
    return create(client, name, LOGS);
  }

  /** Creates a traces data stream and loads the base spans. */
  public static OtelDataStream traces(RestClient client, String name) throws IOException {
    return create(client, name, TRACES);
  }

  private static OtelDataStream create(RestClient client, String name, Signal signal)
      throws IOException {
    OtelDataStream stream = new OtelDataStream(client, name, signal);
    stream.delete();
    try {
      stream.putTemplate(stream.template);
      performRequest(client, new Request("PUT", "/_data_stream/" + name));
      loadDataByRestClient(client, name, signal.dataFile());
      stream.resolvedMapping = stream.readResolvedMapping();
    } catch (RuntimeException | IOException e) {
      try {
        stream.delete();
      } catch (RuntimeException | IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
    return stream;
  }

  /**
   * One field changed, removed or added at a rollover. Each entry in {@code values} becomes one
   * drift document. A null mapping removes the field from the template and from those documents. An
   * added field is one 000001 does not map.
   */
  public record Drift(String field, JSONObject mapping, List<Object> values, boolean adds) {
    /** Keeps the base document's own value, for a type change its value already fits. */
    public static final Object KEEP = new Object();

    public Drift(String field, JSONObject mapping, List<Object> values) {
      this(field, mapping, values, false);
    }

    public Drift {
      if (field == null || field.isEmpty() || Arrays.asList(field.split("\\.", -1)).contains("")) {
        throw new IllegalArgumentException("A drift needs a dotted field path, got " + field);
      }
      if ("@timestamp".equals(field)) {
        throw new IllegalArgumentException("A data stream requires @timestamp as a date");
      }
      if (values == null || values.isEmpty()) {
        throw new IllegalArgumentException("A drift needs at least one document");
      }
      if (adds && (mapping == null || values.contains(KEEP))) {
        throw new IllegalArgumentException(
            "An added field needs a type and cannot keep a base value");
      }
      if (mapping == null && values.stream().anyMatch(Objects::nonNull)) {
        throw new IllegalArgumentException("A removed field takes no values");
      }
      if (mapping != null && mapping.isEmpty()) {
        throw new IllegalArgumentException("A drift that maps the field needs a type");
      }
      mapping = mapping == null ? null : new JSONObject(mapping.toString());
      values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    @Override
    public JSONObject mapping() {
      return mapping == null ? null : new JSONObject(mapping.toString());
    }

    public static Drift changeType(String field, String type, Object... values) {
      return new Drift(field, new JSONObject().put("type", type), Arrays.asList(values));
    }

    public static Drift changeTypeKeepingValues(String field, String type, int count) {
      return new Drift(field, new JSONObject().put("type", type), Collections.nCopies(count, KEEP));
    }

    public static Drift remove(String field, int count) {
      return new Drift(field, null, Collections.nCopies(count, null));
    }

    /** A null value maps the field in the new index without filling it in that document. */
    public static Drift add(String field, String type, Object... values) {
      return new Drift(field, new JSONObject().put("type", type), Arrays.asList(values), true);
    }
  }

  /**
   * Applies the drift to a copy of 000001's resolved mapping, rolls over, and loads the drift
   * documents. Drifts do not accumulate. A change to object or nested keeps 000001's subfields
   * unless it names its own. Throws if the new index ends up mapped exactly like 000001.
   */
  public void rollover(Drift drift) throws IOException {
    boolean mapped = isMapped(resolvedMapping.getJSONObject("properties"), drift.field());
    if (drift.adds() && mapped) {
      throw new IllegalArgumentException(drift.field() + " is already mapped in 000001");
    }
    if (drift.adds() && !underObjects(resolvedMapping.getJSONObject("properties"), drift.field())) {
      throw new IllegalArgumentException(
          drift.field() + " sits under a field 000001 maps as a value, so it cannot be added");
    }
    if (!drift.adds() && !mapped) {
      throw new IllegalArgumentException(
          drift.field()
              + " is not a mapped property in 000001, a multi-field drifts with its parent,"
              + " and Drift.add adds a field 000001 lacks");
    }
    List<JSONObject> docs = driftDocs(drift);
    JSONObject drifted = new JSONObject(template.toString());
    drifted.getJSONObject("template").put("mappings", new JSONObject(resolvedMapping.toString()));
    JSONObject properties =
        drifted.getJSONObject("template").getJSONObject("mappings").getJSONObject("properties");
    if (drift.mapping() == null) {
      removeMapping(properties, drift.field());
    } else if (drift.adds()) {
      putMapping(properties, drift.field(), drift.mapping());
    } else {
      JSONObject mapping = drift.mapping();
      JSONObject base = mappingAt(resolvedMapping.getJSONObject("properties"), drift.field());
      if (List.of("object", "nested").contains(mapping.optString("type", "object"))
          && !mapping.has("properties")
          && base.has("properties")) {
        mapping.put("properties", new JSONObject(base.getJSONObject("properties").toString()));
      }
      putMapping(properties, drift.field(), mapping);
    }
    putTemplate(drifted);
    JSONObject rolled =
        new JSONObject(
            getResponseBody(
                performRequest(client, new Request("POST", "/" + name + "/_rollover"))));
    if (!rolled.optBoolean("rolled_over", false)) {
      throw new IllegalStateException(name + " did not roll over: " + rolled);
    }
    generation++;
    loadDocs(docs);
    String latest = rolled.getString("new_index");
    JSONObject loaded =
        new JSONObject(
                getResponseBody(
                    performRequest(client, new Request("GET", "/" + latest + "/_mapping"))))
            .getJSONObject(latest)
            .getJSONObject("mappings");
    loaded.remove("_data_stream_timestamp");
    if (loaded.similar(resolvedMapping)) {
      throw new IllegalStateException(
          drift.field()
              + " drifted to the mapping 000001 already has, "
              + latest
              + " differs in nothing");
    }
  }

  /** Backing index names, oldest first. */
  public List<String> backingIndices() throws IOException {
    JSONObject body =
        new JSONObject(
            getResponseBody(performRequest(client, new Request("GET", "/_data_stream/" + name))));
    JSONArray indices = body.getJSONArray("data_streams").getJSONObject(0).getJSONArray("indices");
    List<String> names = new ArrayList<>();
    for (int i = 0; i < indices.length(); i++) {
      names.add(indices.getJSONObject(i).getString("index_name"));
    }
    return names;
  }

  /** Deletes the data stream, with its backing indices, and the template. */
  public void delete() throws IOException {
    try {
      deleteIfExists("/_data_stream/" + name);
    } catch (IOException | RuntimeException e) {
      try {
        deleteIfExists("/_index_template/" + name);
      } catch (IOException | RuntimeException template) {
        e.addSuppressed(template);
      }
      throw e;
    }
    deleteIfExists("/_index_template/" + name);
  }

  private void putTemplate(JSONObject body) {
    Request request = new Request("PUT", "/_index_template/" + name);
    request.setJsonEntity(body.toString());
    performRequest(client, request);
  }

  private void deleteIfExists(String path) throws IOException {
    try {
      client.performRequest(new Request("DELETE", path));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  /**
   * Base documents that carry the field come first, so a type change replaces a real value, and an
   * empty value such as a span's {@code links: []} or {@code traceState: ""} carries nothing. Each
   * drift document's {@code @timestamp} falls after every base document and every earlier drift,
   * and the signal's time fields move with it, except inside a value the drift supplies. Other
   * fields, {@code observedTimestamp} included, keep the base document's value.
   */
  private List<JSONObject> driftDocs(Drift drift) {
    List<JSONObject> sources = new ArrayList<>();
    List<JSONObject> lacking = new ArrayList<>();
    baseDocs.forEach(d -> (carries(d, drift.field()) ? sources : lacking).add(d));
    int carrying = sources.size();
    sources.addAll(lacking);
    if (drift.values().lastIndexOf(Drift.KEEP) >= carrying) {
      throw new IllegalArgumentException(
          "Only " + carrying + " base documents carry " + drift.field() + " to keep");
    }
    Instant start =
        baseDocs.stream()
            .map(d -> Instant.parse(d.getString("@timestamp")))
            .max(Instant::compareTo)
            .orElseThrow();
    List<JSONObject> docs = new ArrayList<>();
    for (int i = 0; i < drift.values().size(); i++) {
      JSONObject doc = new JSONObject(sources.get(i % sources.size()).toString());
      Object value = drift.values().get(i);
      List<Slot> slots = find(doc, drift.field());
      String type = drift.mapping() == null ? null : drift.mapping().optString("type", "object");
      boolean objectType = type != null && OBJECT_TYPES.contains(type);
      if (value == Drift.KEEP
          && objectType
          && slots.stream().anyMatch(s -> !holdsObject(s) && !isEmpty(s))) {
        throw new IllegalArgumentException(
            drift.field() + " holds a value, which a change to an object type cannot keep");
      }
      if (value == Drift.KEEP
          && !"object".equals(type)
          && slots.stream().anyMatch(s -> !s.keys().equals(List.of(s.key())))) {
        throw new IllegalArgumentException(
            drift.field() + " is stored as flattened keys, which only a change to object can keep");
      }
      if (value == Drift.KEEP
          && !objectType
          && slots.stream().anyMatch(OtelDataStream::holdsObject)) {
        throw new IllegalArgumentException(
            drift.field() + " holds an object, which a change to a scalar type cannot keep");
      }
      if (value != Drift.KEEP) {
        slots.forEach(s -> s.keys().forEach(s.parent()::remove));
        if (drift.mapping() != null) {
          List<Slot> targets = slots.isEmpty() ? List.of(place(doc, drift.field())) : slots;
          targets.forEach(t -> t.parent().put(t.key(), copy(value)));
        }
      }
      Instant timestamp = start.plusSeconds((generation + 1) * 3600L + i);
      Duration shift = Duration.between(Instant.parse(doc.getString("@timestamp")), timestamp);
      doc.put("@timestamp", timestamp.toString());
      for (String field : signal.timeFields()) {
        if (value == Drift.KEEP
            || !(field.equals(drift.field()) || field.startsWith(drift.field() + "."))) {
          for (Slot s : find(doc, field)) {
            if (s.parent().opt(s.key()) instanceof String time) {
              s.parent().put(s.key(), Instant.parse(time).plus(shift).toString());
            }
          }
        }
      }
      docs.add(doc);
    }
    return docs;
  }

  private void loadDocs(List<JSONObject> docs) throws IOException {
    StringBuilder body = new StringBuilder();
    for (JSONObject doc : docs) {
      body.append("{\"create\": {}}\n").append(doc).append('\n');
    }
    Request request = new Request("POST", "/" + name + "/_bulk?refresh=true");
    request.setJsonEntity(body.toString());
    JSONObject response = new JSONObject(getResponseBody(performRequest(client, request)));
    if (response.optBoolean("errors", false)) {
      throw new IllegalStateException("Drift bulk load into " + name + " failed: " + response);
    }
  }

  /** 000001's mapping after the base load, without the metadata field the data stream adds. */
  private JSONObject readResolvedMapping() throws IOException {
    JSONObject body =
        new JSONObject(
            getResponseBody(performRequest(client, new Request("GET", "/" + name + "/_mapping"))));
    if (body.length() != 1) {
      throw new IllegalStateException(
          name + " should have one backing index, got " + body.keySet());
    }
    JSONObject mapping = body.getJSONObject(body.keys().next()).getJSONObject("mappings");
    mapping.remove("_data_stream_timestamp");
    return mapping;
  }

  private static List<JSONObject> readBaseDocs(String dataFile) throws IOException {
    List<JSONObject> docs = new ArrayList<>();
    for (String line : Files.readAllLines(Paths.get(getResourceFilePath(dataFile)))) {
      if (line.isBlank()) {
        continue;
      }
      JSONObject json = new JSONObject(line);
      if (!(json.length() == 1 && (json.has("create") || json.has("index")))) {
        docs.add(json);
      }
    }
    if (docs.isEmpty()) {
      throw new IllegalStateException(dataFile + " holds no documents");
    }
    return docs;
  }

  /** Where a field sits in a document, and every key holding it or its flattened children. */
  private record Slot(JSONObject parent, String key, List<String> keys) {}

  /**
   * Finds a field in a document whose keys may hold dots, such as {@code "service.name"} under
   * {@code resource.attributes}. An object stored as flattened keys, such as {@code
   * resource.attributes.service}, is found through those keys. Under an array of objects, such as
   * nested {@code events}, every element carrying the field is a match. The shallowest match wins,
   * so a subtree stored both nested and flattened in one document is only half found. Returns an
   * empty list if absent.
   */
  private static List<Slot> find(JSONObject obj, String path) {
    List<String> keys = new ArrayList<>();
    for (String key : obj.keySet()) {
      if (key.equals(path) || key.startsWith(path + ".")) {
        keys.add(key);
      }
    }
    if (!keys.isEmpty()) {
      return List.of(new Slot(obj, path, keys));
    }
    for (String key : obj.keySet()) {
      if (path.startsWith(key + ".")) {
        List<Slot> found = new ArrayList<>();
        for (JSONObject child : children(obj.get(key))) {
          found.addAll(find(child, path.substring(key.length() + 1)));
        }
        if (!found.isEmpty()) {
          return found;
        }
      }
    }
    return List.of();
  }

  /**
   * The deepest object a document already holds along the path, with the rest as one key. A root
   * {@code "attributes.a.b"} beside an {@code attributes} object is indexed but not returned by the
   * fields API, so a value goes inside the existing object instead. Under an array the value goes
   * in its first object, or in a new one if the array is empty.
   */
  private static Slot place(JSONObject obj, String path) {
    for (String key : obj.keySet()) {
      if (path.startsWith(key + ".")) {
        String rest = path.substring(key.length() + 1);
        Object value = obj.get(key);
        if (value instanceof JSONObject child) {
          return place(child, rest);
        }
        if (value instanceof JSONArray array && !children(array).isEmpty()) {
          return place(children(array).get(0), rest);
        }
        if (value instanceof JSONArray array && array.isEmpty()) {
          JSONObject element = new JSONObject();
          array.put(element);
          return new Slot(element, rest, List.of());
        }
      }
    }
    return new Slot(obj, path, List.of());
  }

  /** Whether the field holds a value, so not only an empty string, array or object, or a null. */
  private static boolean carries(JSONObject doc, String path) {
    return find(doc, path).stream().anyMatch(s -> !isEmpty(s));
  }

  private static boolean isEmpty(Slot s) {
    Object value = s.parent().opt(s.key());
    return s.keys().equals(List.of(s.key()))
        && (value == null
            || value == JSONObject.NULL
            || "".equals(value)
            || (value instanceof JSONArray array && array.isEmpty())
            || (value instanceof JSONObject object && object.isEmpty()));
  }

  /** Whether the slot holds an object, as flattened keys, an object, or an array of objects. */
  private static boolean holdsObject(Slot s) {
    return !s.keys().equals(List.of(s.key())) || !children(s.parent().opt(s.key())).isEmpty();
  }

  /** A drift value to store, copied so a shared object is neither aliased nor shifted twice. */
  private static Object copy(Object value) {
    if (value == null) {
      return JSONObject.NULL;
    }
    if (value instanceof JSONObject object) {
      return new JSONObject(object.toString());
    }
    return value instanceof JSONArray array ? new JSONArray(array.toString()) : value;
  }

  /** The object a value is, or the objects in the array it is. */
  private static List<JSONObject> children(Object value) {
    List<JSONObject> children = new ArrayList<>();
    if (value instanceof JSONObject child) {
      children.add(child);
    } else if (value instanceof JSONArray array) {
      for (Object element : array) {
        if (element instanceof JSONObject child) {
          children.add(child);
        }
      }
    }
    return children;
  }

  private static void putMapping(JSONObject properties, String path, JSONObject mapping) {
    String[] parts = path.split("\\.");
    JSONObject current = properties;
    for (int i = 0; i < parts.length - 1; i++) {
      JSONObject child = current.optJSONObject(parts[i]);
      if (child == null) {
        child = new JSONObject();
        current.put(parts[i], child);
      }
      if (!child.has("properties")) {
        child.put("properties", new JSONObject());
      }
      current = child.getJSONObject("properties");
    }
    current.put(parts[parts.length - 1], mapping);
  }

  private static JSONObject mappingAt(JSONObject properties, String path) {
    String[] parts = path.split("\\.");
    JSONObject node = properties.getJSONObject(parts[0]);
    for (int i = 1; i < parts.length; i++) {
      node = node.getJSONObject("properties").getJSONObject(parts[i]);
    }
    return node;
  }

  /** Whether every parent of the path that 000001 maps is an object or nested. */
  private static boolean underObjects(JSONObject properties, String path) {
    String[] parts = path.split("\\.");
    JSONObject current = properties;
    for (int i = 0; i < parts.length - 1 && current != null; i++) {
      JSONObject child = current.optJSONObject(parts[i]);
      if (child == null) {
        return true;
      }
      if (!List.of("object", "nested").contains(child.optString("type", "object"))) {
        return false;
      }
      current = child.optJSONObject("properties");
    }
    return true;
  }

  private static boolean isMapped(JSONObject properties, String path) {
    String[] parts = path.split("\\.");
    JSONObject current = properties;
    for (int i = 0; i < parts.length - 1 && current != null; i++) {
      JSONObject child = current.optJSONObject(parts[i]);
      current = child == null ? null : child.optJSONObject("properties");
    }
    return current != null && current.has(parts[parts.length - 1]);
  }

  private static void removeMapping(JSONObject properties, String path) {
    String[] parts = path.split("\\.");
    JSONObject current = properties;
    for (int i = 0; i < parts.length - 1 && current != null; i++) {
      JSONObject child = current.optJSONObject(parts[i]);
      current = child == null ? null : child.optJSONObject("properties");
    }
    if (current != null) {
      current.remove(parts[parts.length - 1]);
    }
  }
}
