/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.domain;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.json.JSONObject;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

@RequiredArgsConstructor
public class PPLQueryRequest extends ActionRequest {
  public static final PPLQueryRequest NULL = new PPLQueryRequest("", null, "", "");

  private final String pplQuery;
  @Getter
  private final JSONObject jsonContent;
  @Getter
  @Accessors(fluent = true)
  private final String path;
  @Getter
  private String format = "";

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private JsonResponseFormatter.Style style = JsonResponseFormatter.Style.COMPACT;

  /**
   * Constructor of PPLQueryRequest.
   */
  public PPLQueryRequest(String pplQuery, JSONObject jsonContent, String path, String format) {
    this.pplQuery = pplQuery;
    this.jsonContent = jsonContent;
    this.path = path;
    this.format = format;
  }

  public PPLQueryRequest(StreamInput in) throws IOException {
    super(in);
    pplQuery = in.readString();
    format = in.readString();
    jsonContent = new JSONObject(in.readString());
    path = in.readString();
    sanitize = in.readBoolean();
    style = in.readEnum(JsonResponseFormatter.Style.class);
  }


  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(pplQuery);
    out.writeString(format);
    out.writeString(jsonContent.toString());
    out.writeString(path);
    out.writeBoolean(sanitize);
    out.writeEnum(style);
  }

  public String getRequest() {
    return pplQuery;
  }

  /**
   * Check if request is to explain rather than execute the query.
   * @return  true if it is a explain request
   */
  public boolean isExplainRequest() {
    return path.endsWith("/_explain");
  }

  /**
   * Decide on the formatter by the requested format.
   */
  public Format format() {
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT,"response in %s format is not supported.", format));
    }
  }

  @Override
  public ActionRequestValidationException validate() {
    if (pplQuery.isEmpty()) {
      return ValidateActions.addValidationError("query is empty", null);
    }
    return null;
  }

  public static PPLQueryRequest fromActionRequest(final ActionRequest actionRequest) {
    if (actionRequest instanceof PPLQueryRequest) {
      return (PPLQueryRequest) actionRequest;
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
      actionRequest.writeTo(osso);
      try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
        return new PPLQueryRequest(input);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to parse ActionRequest into PPLQueryRequest", e);
    }
  }
}
