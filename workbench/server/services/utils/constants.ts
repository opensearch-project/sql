/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import { ParsedUrlQuery } from 'querystring';

export const SQL_TRANSLATE_ROUTE = `/_plugins/_sql/_explain`;
export const PPL_TRANSLATE_ROUTE = `/_plugins/_ppl/_explain`;
export const SQL_QUERY_ROUTE = `/_plugins/_sql`;
export const PPL_QUERY_ROUTE = `/_plugins/_ppl`;
export const FORMAT_CSV = `format=csv`;
export const FORMAT_JSON = `format=json`;
export const FORMAT_TEXT = `format=raw`;

export const DEFAULT_HEADERS = {
  'Content-Type': 'application/json',
  Accept: 'application/json',
  'User-Agent': 'OpenSearch-Dashboards',
};

export const CLUSTER = {
  ADMIN: 'admin',
  SQL: 'opensearch-sql',
  DATA: 'data',
};
