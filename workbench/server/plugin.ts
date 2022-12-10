/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import {
  PluginInitializerContext,
  CoreSetup,
  CoreStart,
  Plugin,
  Logger,
  ILegacyClusterClient,
} from '../../../src/core/server';

import { WorkbenchPluginSetup, WorkbenchPluginStart } from './types';
import defineRoutes from './routes';
import sqlPlugin from './clusters/sql/sqlPlugin'; 


export class WorkbenchPlugin implements Plugin<WorkbenchPluginSetup, WorkbenchPluginStart> {
  private readonly logger: Logger;

  constructor(initializerContext: PluginInitializerContext) {
    this.logger = initializerContext.logger.get();
  }

  public setup(core: CoreSetup) {
    this.logger.debug('queryWorkbenchDashboards: Setup');
    const router = core.http.createRouter();
    const client: ILegacyClusterClient = core.opensearch.legacy.createClient(
      'query_workbench',
      {
        plugins: [sqlPlugin]
      }
    )

    // Register server side APIs
    defineRoutes(router, client);

    return {};
  }

  public start(core: CoreStart) {
    this.logger.debug('queryWorkbenchDashboards: Started');
    return {};
  }

  public stop() {}
}
