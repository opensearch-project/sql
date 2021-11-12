/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import { AppMountParameters, CoreSetup, CoreStart, Plugin } from '../../../src/core/public';
import { WorkbenchPluginSetup, WorkbenchPluginStart, AppPluginStartDependencies } from './types';
import { PLUGIN_NAME } from '../common';

export class WorkbenchPlugin implements Plugin<WorkbenchPluginSetup, WorkbenchPluginStart> {
  public setup(core: CoreSetup): WorkbenchPluginSetup {
    // Register an application into the side navigation menu
    core.application.register({
      id: 'opensearch-query-workbench',
      title: PLUGIN_NAME,
      category: {
        id: 'opensearch',
        label: 'OpenSearch Plugins',
        order: 2000,
      },
      order: 1000,
      async mount(params: AppMountParameters) {
        // Load application bundle
        const { renderApp } = await import('./application');
        // Get start services as specified in opensearch_dashboards.json
        const [coreStart, depsStart] = await core.getStartServices();
        // Render the application
        return renderApp(coreStart, depsStart as AppPluginStartDependencies, params);
      },
    });

    // Return methods that should be available to other plugins
    return {};
  }

  public start(core: CoreStart): WorkbenchPluginStart {
    return {};
  }

  public stop() {}
}
