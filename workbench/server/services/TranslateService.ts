/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


export default class TranslateService {
  private client: any;

  constructor(client: any) {
    this.client = client;
  }

  translateSQL = async (request: any) => {
    try {
      const queryRequest = {
        query: request.body.query,
      };

      const params = {
        body: JSON.stringify(queryRequest),
      };

      const queryResponse = await this.client
        .asScoped(request)
        .callAsCurrentUser('sql.translateSQL', params);
      const ret = {
        data: {
          ok: true,
          resp: queryResponse,
        },
      };
      return ret;
    } catch (err) {
      console.log(err);
      return {
        data: {
          ok: false,
          resp: err.message,
        },
      };
    }
  };

  translatePPL = async (request: any) => {
    try {
      const queryRequest = {
        query: request.body.query,
      };

      const params = {
        body: JSON.stringify(queryRequest),
      };

      const queryResponse = await this.client
        .asScoped(request)
        .callAsCurrentUser('sql.translatePPL', params);
      return {
        data: {
          ok: true,
          resp: queryResponse,
        },
      };
    } catch (err) {
      console.log(err);
      return {
        data: {
          ok: false,
          resp: err.message,
        },
      };
    }
  };
}
