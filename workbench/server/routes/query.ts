/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import { schema } from '@osd/config-schema';
import { IOpenSearchDashboardsResponse, IRouter, ResponseError } from '../../../../src/core/server';
import QueryService from '../services/QueryService';
import {
  ROUTE_PATH_SQL_QUERY,
  ROUTE_PATH_PPL_QUERY,
  ROUTE_PATH_SQL_CSV,
  ROUTE_PATH_SQL_JSON,
  ROUTE_PATH_SQL_TEXT,
  ROUTE_PATH_PPL_CSV,
  ROUTE_PATH_PPL_JSON,
  ROUTE_PATH_PPL_TEXT,
} from '../utils/constants';

export default function query(server: IRouter, service: QueryService) {
  server.post(
    {
      path: ROUTE_PATH_SQL_QUERY,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describeSQLQuery(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_PPL_QUERY,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describePPLQuery(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_SQL_CSV,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describeSQLCsv(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_PPL_CSV,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describePPLCsv(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_SQL_JSON,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describeSQLJson(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_PPL_JSON,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describePPLJson(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_SQL_TEXT,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describeSQLText(request);
      return response.ok({
        body: retVal,
      });
    }
  );

  server.post(
    {
      path: ROUTE_PATH_PPL_TEXT,
      validate: {
        body: schema.any(),
      },
    },
    async (context, request, response): Promise<IOpenSearchDashboardsResponse<any | ResponseError>> => {
      const retVal = await service.describePPLText(request);
      return response.ok({
        body: retVal,
      });
    }
  );
}
