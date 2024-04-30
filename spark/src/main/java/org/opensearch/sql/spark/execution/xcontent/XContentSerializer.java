/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import java.io.IOException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Interface for XContentSerializer */
public interface XContentSerializer<T extends StateModel> {

  /**
   * Serializes the given object to an XContentBuilder using the specified parameters.
   *
   * @param object The object to serialize.
   * @param params The parameters to use for serialization.
   * @return An XContentBuilder containing the serialized representation of the object.
   * @throws IOException If an I/O error occurs during serialization.
   */
  XContentBuilder toXContent(T object, ToXContent.Params params) throws IOException;

  /**
   * Deserializes an object from an XContentParser.
   *
   * @param parser The XContentParser to read the object from.
   * @param seqNo The sequence number associated with the object.
   * @param primaryTerm The primary term associated with the object.
   * @return The deserialized object.
   */
  T fromXContent(XContentParser parser, long seqNo, long primaryTerm);
}
