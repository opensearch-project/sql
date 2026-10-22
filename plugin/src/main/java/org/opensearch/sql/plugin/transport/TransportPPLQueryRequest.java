/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.json.JSONObject;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

@RequiredArgsConstructor
public class TransportPPLQueryRequest extends ActionRequest {
  public static final TransportPPLQueryRequest NULL = new TransportPPLQueryRequest("", null, "");
  private final String pplQuery;
  @Getter private final JSONObject jsonContent;

  @Getter private final String path;

  @Getter private String format = "";
  @Getter private String explainMode;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private JsonResponseFormatter.Style style = JsonResponseFormatter.Style.COMPACT;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean profile = false;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean analyze = false;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private String queryId = null;

  /**
   * Caller's {@code wait_for_completion_timeout}, {@code null} when the field was not present.
   * Presence of this field or {@link #keepAlive} switches the transport action to the async submit
   * path.
   */
  @Setter
  @Getter
  @Accessors(fluent = true)
  private Duration waitForCompletionTimeout = null;

  /**
   * Caller's {@code keep_alive}, {@code null} when the field was not present. Presence of this
   * field or {@link #waitForCompletionTimeout} switches the transport action to the async submit
   * path.
   */
  @Setter
  @Getter
  @Accessors(fluent = true)
  private Duration keepAlive = null;

  /** Constructor of TransportPPLQueryRequest from PPLQueryRequest. */
  public TransportPPLQueryRequest(PPLQueryRequest pplQueryRequest) {
    pplQuery = pplQueryRequest.getRequest();
    jsonContent = pplQueryRequest.getJsonContent();
    path = pplQueryRequest.getPath();
    format = pplQueryRequest.getFormat();
    sanitize = pplQueryRequest.sanitize();
    style = pplQueryRequest.style();
    profile = pplQueryRequest.profile();
    analyze = pplQueryRequest.analyze();
    explainMode = pplQueryRequest.mode().getModeName();
    queryId = pplQueryRequest.queryId();
    waitForCompletionTimeout = pplQueryRequest.waitForCompletionTimeout();
    keepAlive = pplQueryRequest.keepAlive();
  }

  /** Constructor of TransportPPLQueryRequest from StreamInput. */
  public TransportPPLQueryRequest(StreamInput in) throws IOException {
    super(in);
    pplQuery = in.readOptionalString();
    format = in.readOptionalString();
    explainMode = in.readOptionalString();
    String jsonContentString = in.readOptionalString();
    jsonContent = jsonContentString != null ? new JSONObject(jsonContentString) : null;
    path = in.readOptionalString();
    sanitize = in.readBoolean();
    style = in.readEnum(JsonResponseFormatter.Style.class);
    profile = in.readBoolean();
    analyze = in.readBoolean();
    queryId = in.readOptionalString();
    Long waitMillis = in.readOptionalLong();
    waitForCompletionTimeout = waitMillis == null ? null : Duration.ofMillis(waitMillis);
    Long keepAliveMillis = in.readOptionalLong();
    keepAlive = keepAliveMillis == null ? null : Duration.ofMillis(keepAliveMillis);
  }

  /** Re-create the object from the actionRequest. */
  public static TransportPPLQueryRequest fromActionRequest(final ActionRequest actionRequest) {
    if (actionRequest instanceof TransportPPLQueryRequest) {
      return (TransportPPLQueryRequest) actionRequest;
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
      actionRequest.writeTo(osso);
      try (InputStreamStreamInput input =
          new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
        return new TransportPPLQueryRequest(input);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "failed to parse ActionRequest into TransportPPLQueryRequest", e);
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeOptionalString(pplQuery);
    out.writeOptionalString(format);
    out.writeOptionalString(explainMode);
    out.writeOptionalString(jsonContent != null ? jsonContent.toString() : null);
    out.writeOptionalString(path);
    out.writeBoolean(sanitize);
    out.writeEnum(style);
    out.writeBoolean(profile);
    out.writeBoolean(analyze);
    out.writeOptionalString(queryId);
    out.writeOptionalLong(
        waitForCompletionTimeout == null ? null : waitForCompletionTimeout.toMillis());
    out.writeOptionalLong(keepAlive == null ? null : keepAlive.toMillis());
  }

  public String getRequest() {
    return pplQuery;
  }

  /**
   * Check if request is to explain rather than execute the query.
   *
   * @return true if it is an explain request
   */
  public boolean isExplainRequest() {
    return path != null && path.endsWith("/_explain");
  }

  /**
   * Check if request is for grammar metadata endpoint.
   *
   * @return true if it is a grammar metadata request
   */
  public boolean isGrammarRequest() {
    return path != null && path.endsWith("/_grammar");
  }

  /** Decide on the formatter by the requested format. */
  public Format format() {
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "response in %s format is not supported.", format));
    }
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  @Override
  public PPLQueryTask createTask(
      long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
    return new PPLQueryTask(id, type, action, getDescription(), parentTaskId, headers);
  }

  @Override
  public String getDescription() {
    String prefix = (queryId != null) ? "PPL [queryId=" + queryId + "]: " : "PPL: ";
    return prefix + pplQuery;
  }

  /** Convert to PPLQueryRequest. */
  public PPLQueryRequest toPPLQueryRequest() {
    PPLQueryRequest pplQueryRequest =
        new PPLQueryRequest(pplQuery, jsonContent, path, format, explainMode, profile, analyze);
    pplQueryRequest.sanitize(sanitize);
    pplQueryRequest.style(style);
    pplQueryRequest.queryId(queryId);
    pplQueryRequest.waitForCompletionTimeout(waitForCompletionTimeout);
    pplQueryRequest.keepAlive(keepAlive);
    return pplQueryRequest;
  }

  /** Presence of either async body field selects the asynchronous submit path. */
  public boolean isAsync() {
    return waitForCompletionTimeout != null || keepAlive != null;
  }
}
