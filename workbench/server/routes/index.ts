/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import { ILegacyClusterClient, IRouter } from '../../../../src/core/server';
import registerTranslateRoute from './translate';
import registerQueryRoute from './query';
import TranslateService from '../services/TranslateService';
import QueryService from '../services/QueryService';


export default function (router: IRouter, client: ILegacyClusterClient) {
  const translateService = new TranslateService(client);
  registerTranslateRoute(router, translateService);

  const queryService = new QueryService(client);
  registerQueryRoute(router, queryService);
}
