/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.streaming;

import java.util.Set;
import lombok.Data;
import org.apache.hadoop.fs.Path;

/**
 * File metadata. Batch id associate with the set of {@link Path}.
 */
@Data
public class FileMetaData {

  private final Long batchId;

  private final Set<Path> paths;
}
