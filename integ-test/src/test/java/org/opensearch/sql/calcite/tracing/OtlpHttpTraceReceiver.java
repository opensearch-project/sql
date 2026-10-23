/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tracing;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * In-process OTLP-HTTP trace receiver for tests. Binds {@link HttpServer} on {@code 0.0.0.0:<port>}
 * at path {@code /v1/traces}, accepts POST requests with OTLP-protobuf bodies (gzip-encoded or
 * plain), decodes them via the generated {@link ExportTraceServiceRequest} protobuf classes, and
 * exposes filter/wait helpers.
 *
 * <p>Pairs with a cluster running the OpenSearch {@code telemetry-otel} plugin. The plugin's {@code
 * OTelSpanExporterFactory} instantiates {@code OtlpHttpSpanExporter.getDefault()} which is
 * unconditionally hardcoded to {@code http://localhost:4318/v1/traces} and uses {@code
 * application/x-protobuf} content type. The tests therefore MUST bind port 4318 and MUST speak
 * protobuf — env-var / sysprop endpoint overrides are ignored by the plugin, and the exporter
 * cannot be forced into JSON mode without changing the exporter's builder call.
 *
 * <p>{@code BatchSpanProcessor} flushes on a schedule (defaults to 5s; the cluster override for
 * tracingIntegTest is 500ms), so tests should use {@link #waitForSpans} rather than assuming
 * synchronous delivery.
 */
public final class OtlpHttpTraceReceiver implements AutoCloseable {

  private final HttpServer server;
  private final List<Span> spans = Collections.synchronizedList(new ArrayList<>());

  public OtlpHttpTraceReceiver(int port) throws IOException {
    // Bind to 0.0.0.0 so both IPv4 and IPv6 localhost lookups from the cluster JVM reach us.
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    this.server.createContext("/v1/traces", this::handle);
    // Also mount on root as a defensive net.
    this.server.createContext("/", this::handle);
    this.server.setExecutor(null);
    this.server.start();
  }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      if (!"POST".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      // Read the whole body into a byte[] first — HttpServer's request InputStream can hang if
      // parseFrom() reads past the Content-Length boundary on a keep-alive connection.
      byte[] bytes = exchange.getRequestBody().readAllBytes();
      String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
      if ("gzip".equalsIgnoreCase(encoding)) {
        try (GZIPInputStream gz = new GZIPInputStream(new java.io.ByteArrayInputStream(bytes))) {
          bytes = gz.readAllBytes();
        }
      }
      ExportTraceServiceRequest req = ExportTraceServiceRequest.parseFrom(bytes);
      for (ResourceSpans rs : req.getResourceSpansList()) {
        for (ScopeSpans ss : rs.getScopeSpansList()) {
          for (io.opentelemetry.proto.trace.v1.Span sp : ss.getSpansList()) {
            spans.add(parseSpan(sp));
          }
        }
      }
      byte[] resp = new byte[0];
      exchange.getResponseHeaders().add("Content-Type", "application/x-protobuf");
      exchange.sendResponseHeaders(200, resp.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(resp);
      }
    } catch (Throwable e) {
      byte[] resp = ("{\"error\":\"" + e.getMessage() + "\"}").getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(400, resp.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(resp);
      }
    }
  }

  private static Span parseSpan(io.opentelemetry.proto.trace.v1.Span p) {
    Span s = new Span();
    s.traceId = HexFormat.of().formatHex(p.getTraceId().toByteArray());
    s.spanId = HexFormat.of().formatHex(p.getSpanId().toByteArray());
    s.parentSpanId =
        p.getParentSpanId().isEmpty()
            ? ""
            : HexFormat.of().formatHex(p.getParentSpanId().toByteArray());
    s.name = p.getName();
    // OTLP SpanKind proto enum values line up with the OTel spec:
    // 0=UNSPECIFIED, 1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER
    s.kind = p.getKindValue();
    s.startEpochNanos = p.getStartTimeUnixNano();
    s.endEpochNanos = p.getEndTimeUnixNano();
    s.statusCode = p.getStatus().getCodeValue();
    s.statusMessage = p.getStatus().getMessage();
    s.attributes = new HashMap<>();
    for (KeyValue kv : p.getAttributesList()) {
      s.attributes.put(kv.getKey(), stringify(kv.getValue()));
    }
    return s;
  }

  private static String stringify(AnyValue v) {
    switch (v.getValueCase()) {
      case STRING_VALUE:
        return v.getStringValue();
      case BOOL_VALUE:
        return String.valueOf(v.getBoolValue());
      case INT_VALUE:
        return String.valueOf(v.getIntValue());
      case DOUBLE_VALUE:
        return String.valueOf(v.getDoubleValue());
      case ARRAY_VALUE:
        return v.getArrayValue().getValuesList().stream()
            .map(OtlpHttpTraceReceiver::stringify)
            .collect(Collectors.joining(",", "[", "]"));
      default:
        return v.toString();
    }
  }

  /** Snapshot of all spans received so far. Ordered as they arrived on the wire. */
  public List<Span> snapshot() {
    synchronized (spans) {
      return new ArrayList<>(spans);
    }
  }

  /**
   * Poll until {@code minCount} spans matching {@code matcher} arrive, or {@code timeout} elapses.
   * Throws {@link AssertionError} if the deadline expires — includes current snapshot names in the
   * message for diagnosis.
   */
  public List<Span> waitForSpans(Predicate<Span> matcher, int minCount, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      List<Span> matched = snapshot().stream().filter(matcher).collect(Collectors.toList());
      if (matched.size() >= minCount) return matched;
      if (System.nanoTime() >= deadline) {
        throw new AssertionError(
            "Timeout after "
                + timeout
                + " waiting for "
                + minCount
                + " spans matching predicate; got "
                + matched.size()
                + " of "
                + snapshot().size()
                + " total spans. Names seen: "
                + snapshot().stream().map(x -> x.name).collect(Collectors.toList()));
      }
      Thread.sleep(200);
    }
  }

  public void clear() {
    synchronized (spans) {
      spans.clear();
    }
  }

  @Override
  public void close() {
    server.stop(1);
  }

  /** Parsed span record. Public fields for concise test assertions. */
  public static final class Span {
    public String traceId;
    public String spanId;
    public String parentSpanId;
    public String name;

    /** OTLP SpanKind: 0=UNSPECIFIED, 1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER. */
    public int kind;

    public long startEpochNanos;
    public long endEpochNanos;

    /** OTLP StatusCode: 0=UNSET, 1=OK, 2=ERROR. */
    public int statusCode;

    public String statusMessage;
    public Map<String, String> attributes;

    public long durationNanos() {
      return endEpochNanos - startEpochNanos;
    }

    public String attr(String key) {
      return attributes.get(key);
    }

    @Override
    public String toString() {
      return name
          + "["
          + spanId
          + " parent="
          + (parentSpanId.isEmpty() ? "-" : parentSpanId)
          + " kind="
          + kind
          + " status="
          + statusCode
          + "]";
    }
  }
}
