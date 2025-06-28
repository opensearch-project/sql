/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.query.planner.core.Config;
import org.opensearch.transport.client.Client;

/** Handler for Point In Time */
public class PointInTimeHandlerImpl implements PointInTimeHandler {
  private final Client client;
  private String[] indices;
  private final Optional<Config> config;
  @Getter @Setter private String pitId;
  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor for class
   *
   * @param client OpenSearch client
   * @param indices list of indices
   */
  public PointInTimeHandlerImpl(Client client, String[] indices) {
    this.client = client;
    this.indices = indices;
    this.config = Optional.empty();
  }

  /**
   * Enhanced constructor with Config for custom timeout support
   *
   * @param client OpenSearch client
   * @param indices list of indices
   * @param config Configuration object containing custom PIT settings
   */
  public PointInTimeHandlerImpl(Client client, String[] indices, Config config) {
    this.client = client;
    this.indices = indices;
    this.config = Optional.ofNullable(config);
  }

  /**
   * Constructor for class
   *
   * @param client OpenSearch client
   * @param pitId Point In Time ID
   */
  public PointInTimeHandlerImpl(Client client, String pitId) {
    this.client = client;
    this.pitId = pitId;
    this.config = Optional.empty();
  }

  /** Create PIT for given indices */
  @Override
  public void create() {
    TimeValue keepAlive = getEffectiveKeepAlive();

    LOG.info("Creating PIT with keepalive: {} ({}ms)", keepAlive, keepAlive.getMillis());

    CreatePitRequest createPitRequest = new CreatePitRequest(keepAlive, false, indices);
    ActionFuture<CreatePitResponse> execute =
        client.execute(CreatePitAction.INSTANCE, createPitRequest);
    try {
      CreatePitResponse pitResponse = execute.get();
      pitId = pitResponse.getId();
      LOG.debug(
          "Created Point In Time {} with keepalive {} successfully.",
          truncatePitId(pitId),
          keepAlive);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(
          String.format("Error occurred while creating PIT with keepalive %s", keepAlive), e);
    }
  }

  /** Delete PIT */
  @Override
  public void delete() {
    DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
    ActionFuture<DeletePitResponse> execute =
        client.execute(DeletePitAction.INSTANCE, deletePitRequest);
    try {
      DeletePitResponse deletePitResponse = execute.get();
      LOG.debug(
          "Delete Point In Time {} status: {}",
          truncatePitId(pitId),
          deletePitResponse.status().getStatus());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error occurred while deleting PIT.", e);
    }
  }

  /**
   * Get effective keepalive value by checking config first, then falling back to default
   *
   * @return TimeValue for PIT keepalive
   */
  private TimeValue getEffectiveKeepAlive() {
    // First: try to get from config if available
    if (config.isPresent()) {
      Optional<TimeValue> customTimeout = config.get().getCustomPitKeepAlive();
      if (customTimeout.isPresent()) {
        LOG.debug(
            "Using custom PIT keepalive from config: {} ({}ms)",
            customTimeout.get(),
            customTimeout.get().getMillis());
        return customTimeout.get();
      }
    }

    // Fallback: use default
    TimeValue defaultKeepAlive = LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE);
    LOG.debug(
        "Using default PIT keepalive: {} ({}ms)", defaultKeepAlive, defaultKeepAlive.getMillis());
    return defaultKeepAlive;
  }

  /**
   * Truncate PIT ID for logging to improve readability
   *
   * @param pitId the PIT ID to truncate
   * @return truncated PIT ID string
   */
  private String truncatePitId(String pitId) {
    if (pitId == null) return "null";
    if (pitId.length() <= 12) return pitId;
    return pitId.substring(0, 12);
  }
}
