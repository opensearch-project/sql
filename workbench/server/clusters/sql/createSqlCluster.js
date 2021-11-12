/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import sqlPlugin from './sqlPlugin';
import { CLUSTER, DEFAULT_HEADERS } from '../../services/utils/constants';

export default function createSqlCluster(server) {
  const { customHeaders, ...rest } = server.config().get('opensearch');
  server.plugins.opensearch.createCluster(
    CLUSTER.SQL,
    {
      plugins: [ sqlPlugin ],
      customHeaders: { ...customHeaders, ...DEFAULT_HEADERS },
      ...rest,
    }
  );
}
