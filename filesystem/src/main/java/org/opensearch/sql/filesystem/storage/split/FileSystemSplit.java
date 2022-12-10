/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage.split;

import java.util.Set;
import java.util.UUID;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.opensearch.sql.storage.split.Split;

@Data
public class FileSystemSplit implements Split {

  @Getter
  @EqualsAndHashCode.Exclude
  private final String splitId = UUID.randomUUID().toString();

  private final Set<Path> paths;
}
