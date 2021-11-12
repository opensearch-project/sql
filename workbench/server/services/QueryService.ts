/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import 'core-js/stable';
import 'regenerator-runtime/runtime';
import _ from 'lodash';

export default class QueryService {
  private client: any;
  constructor(client: any) {
    this.client = client;
  }

  describeQueryInternal = async (request: any, format: string, responseFormat: string) => {
    try {
      const queryRequest = {
        query: request.body.query,
      };
      const params = {
        body: JSON.stringify(queryRequest),
      };

      const queryResponse = await this.client.asScoped(request).callAsCurrentUser(format, params);
      return {
        data: {
          ok: true,
          resp: _.isEqual(responseFormat, 'json') ? JSON.stringify(queryResponse) : queryResponse,
        },
      };
    } catch (err) {
      console.log(err);
      return {
        data: {
          ok: false,
          resp: err.message,
          body: err.body
        },
      };
    }
  };

  describeSQLQuery = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.sqlQuery', 'json');
  };

  describePPLQuery = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.pplQuery', 'json');
  };

  describeSQLCsv = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.sqlCsv', null);
  };

  describePPLCsv = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.pplCsv', null);
  };

  describeSQLJson = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.sqlJson', 'json');
  };

  describePPLJson = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.pplJson', 'json');
  };

  describeSQLText = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.sqlText', null);
  };

  describePPLText = async (request: any) => {
    return this.describeQueryInternal(request, 'sql.pplText', null);
  };
}
