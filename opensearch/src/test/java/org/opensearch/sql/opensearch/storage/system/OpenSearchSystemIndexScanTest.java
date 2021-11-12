/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;

@ExtendWith(MockitoExtension.class)
class OpenSearchSystemIndexScanTest {

  @Mock
  private OpenSearchSystemRequest request;

  @Test
  public void queryData() {
    when(request.search()).thenReturn(Collections.singletonList(stringValue("text")));
    final OpenSearchSystemIndexScan systemIndexScan = new OpenSearchSystemIndexScan(request);

    systemIndexScan.open();
    assertTrue(systemIndexScan.hasNext());
    assertEquals(stringValue("text"), systemIndexScan.next());
  }

  @Test
  public void explain() {
    when(request.toString()).thenReturn("request");
    final OpenSearchSystemIndexScan systemIndexScan = new OpenSearchSystemIndexScan(request);

    assertEquals("request", systemIndexScan.explain());
  }
}
