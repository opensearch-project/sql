/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import { schema } from '@osd/config-schema';
import { IOpenSearchDashboardsResponse, IRouter, ResponseError } from '../../../../src/core/server';
import TranslateService from '../services/TranslateService';

export default function translate(server: IRouter, service: TranslateService) {
  server.post(
    {
      path: '/api/sql_console/translatesql',
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.translateSQL(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: '/api/sql_console/translateppl',
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.translatePPL(request);
      return response.ok({
        body: retVal,
      });
    }
  );
}
