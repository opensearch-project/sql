/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

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
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
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

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private JsonResponseFormatter.Style style = JsonResponseFormatter.Style.COMPACT;

  /** Constructor of TransportPPLQueryRequest from PPLQueryRequest. */
  public TransportPPLQueryRequest(PPLQueryRequest pplQueryRequest) {
    pplQuery = pplQueryRequest.getRequest();
    jsonContent = pplQueryRequest.getJsonContent();
    path = pplQueryRequest.getPath();
    format = pplQueryRequest.getFormat();
    sanitize = pplQueryRequest.sanitize();
    style = pplQueryRequest.style();
  }

  /** Constructor of TransportPPLQueryRequest from StreamInput. */
  public TransportPPLQueryRequest(StreamInput in) throws IOException {
    super(in);
    pplQuery = in.readOptionalString();
    format = in.readOptionalString();
    String jsonContentString = in.readOptionalString();
    jsonContent = jsonContentString != null ? new JSONObject(jsonContentString) : null;
    path = in.readOptionalString();
    sanitize = in.readBoolean();
    style = in.readEnum(JsonResponseFormatter.Style.class);
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
    out.writeOptionalString(jsonContent != null ? jsonContent.toString() : null);
    out.writeOptionalString(path);
    out.writeBoolean(sanitize);
    out.writeEnum(style);
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
    return path.endsWith("/_explain");
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

  /** Convert to PPLQueryRequest. */
  public PPLQueryRequest toPPLQueryRequest() {
    PPLQueryRequest pplQueryRequest = new PPLQueryRequest(pplQuery, jsonContent, path, format);
    pplQueryRequest.sanitize(sanitize);
    pplQueryRequest.style(style);
    
    // Extract timechart parameters from AST if this is a timechart query
    if (pplQuery != null && pplQuery.toLowerCase().contains("timechart")) {
      extractTimechartParametersFromAST(pplQueryRequest);
    }
    
    return pplQueryRequest;
  }
  
  private void extractTimechartParametersFromAST(PPLQueryRequest pplQueryRequest) {
    try {
      // Parse the query to get AST
      org.opensearch.sql.ppl.antlr.PPLSyntaxParser parser = new org.opensearch.sql.ppl.antlr.PPLSyntaxParser();
      org.antlr.v4.runtime.tree.ParseTree cst = parser.parse(pplQuery);
      org.opensearch.sql.ppl.parser.AstBuilder astBuilder = new org.opensearch.sql.ppl.parser.AstBuilder(pplQuery);
      org.opensearch.sql.ast.tree.UnresolvedPlan plan = astBuilder.visit(cst);
      
      // Find Timechart node in the AST
      org.opensearch.sql.ast.tree.Timechart timechartNode = findTimechartNode(plan);
      if (timechartNode != null) {
        pplQueryRequest.timechartLimit(timechartNode.getLimit());
        pplQueryRequest.timechartUseOther(timechartNode.getUseOther());
      }
    } catch (Exception e) {
      // Fallback to default values if AST parsing fails
      pplQueryRequest.timechartLimit(null);
      pplQueryRequest.timechartUseOther(true);
    }
  }
  
  private org.opensearch.sql.ast.tree.Timechart findTimechartNode(org.opensearch.sql.ast.tree.UnresolvedPlan plan) {
    if (plan instanceof org.opensearch.sql.ast.tree.Timechart) {
      return (org.opensearch.sql.ast.tree.Timechart) plan;
    }
    
    // Search in child nodes
    for (org.opensearch.sql.ast.Node child : plan.getChild()) {
      if (child instanceof org.opensearch.sql.ast.tree.UnresolvedPlan) {
        org.opensearch.sql.ast.tree.Timechart result = findTimechartNode((org.opensearch.sql.ast.tree.UnresolvedPlan) child);
        if (result != null) {
          return result;
        }
      }
    }
    
    return null;
  }
}
