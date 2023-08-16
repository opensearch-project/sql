/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.exceptions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import lombok.Getter;
import org.opensearch.core.rest.RestStatus;

/**
 * Error Message.
 */
public class ErrorMessage {

  protected Throwable exception;

  private final int status;

  @Getter
  private final String type;

  @Getter
  private final String reason;

  @Getter
  private final String details;

  /**
   * Error Message Constructor.
   */
  public ErrorMessage(Throwable exception, int status) {
    this.exception = exception;
    this.status = status;

    this.type = fetchType();
    this.reason = fetchReason();
    this.details = fetchDetails();
  }

  private String fetchType() {
    return exception.getClass().getSimpleName();
  }

  protected String fetchReason() {
    return status == RestStatus.BAD_REQUEST.getStatus()
        ? "Invalid Request"
        : "There was internal problem at backend";
  }

  protected String fetchDetails() {
    // Some exception prints internal information (full class name) which is security concern
    return emptyStringIfNull(exception.getLocalizedMessage());
  }

  private String emptyStringIfNull(String str) {
    return str != null ? str : "";
  }

  @Override
  public String toString() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("status", status);
    jsonObject.add("error", getErrorAsJson());
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(jsonObject);
  }

  private JsonObject getErrorAsJson() {
    JsonObject errorJson = new JsonObject();
    errorJson.addProperty("type", type);
    errorJson.addProperty("reason", reason);
    errorJson.addProperty("details", details);
    return errorJson;
  }
}
