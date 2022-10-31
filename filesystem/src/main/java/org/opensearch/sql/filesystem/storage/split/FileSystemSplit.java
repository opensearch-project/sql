/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
