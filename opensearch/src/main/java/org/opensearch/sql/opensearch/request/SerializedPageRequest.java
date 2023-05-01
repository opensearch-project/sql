/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.io.Serializable;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * An object which stores all required information to issue a subsequent page request.
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
public class SerializedPageRequest implements Serializable {
  /**
   * Scroll ID returned by OpenSearch server. Used by OpenSearch too and refers to a
   * search content. Required to query a next page. Could be changed from page to page.
   * Core part of pagination feature.
   */
  private final String scrollId;

  /**
   * List of extra fields which OpenSearch should return for each doc in the response.
   * Actually is not required for every page, but needed to parse each search response.
   * Can't be changed from page to page.
   * Part of metafields feature.
   */
  private final List<String> includes;
}
