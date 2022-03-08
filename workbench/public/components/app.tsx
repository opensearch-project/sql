/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import React from 'react';
import { I18nProvider } from '@osd/i18n/react';
import { BrowserRouter as Router, Route } from 'react-router-dom';

import { EuiPage, EuiPageBody } from '@elastic/eui';

import { CoreStart } from '../../../../src/core/public';
import { NavigationPublicPluginStart } from '../../../../src/plugins/navigation/public';

import { Main } from './Main';

interface WorkbenchAppDeps {
  basename: string;
  notifications: CoreStart['notifications'];
  http: CoreStart['http'];
  navigation: NavigationPublicPluginStart;
  chrome: CoreStart['chrome'];
}

const onChange = () => {};

export const WorkbenchApp = ({ basename, notifications, http, navigation, chrome }: WorkbenchAppDeps) => {
  return (
    <Router basename={'/' + basename}>
      <I18nProvider>
        <div>
          <EuiPage>
            <EuiPageBody>
              <Route 
                path="/" 
                render={(props) => 
                  <Main 
                    httpClient={http} 
                    {...props} 
                    setBreadcrumbs={chrome.setBreadcrumbs} 
                  />
                } 
              />
            </EuiPageBody>
          </EuiPage>
        </div>
      </I18nProvider>
    </Router>
  );
};
