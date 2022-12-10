/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import { SQL_TRANSLATE_ROUTE, SQL_QUERY_ROUTE, PPL_QUERY_ROUTE, PPL_TRANSLATE_ROUTE, FORMAT_CSV, FROMAT_JDBC, FORMAT_JSON, FORMAT_TEXT } from '../../services/utils/constants';

export default function sqlPlugin(Client, config, components) {
  const ca = components.clientAction.factory;

  Client.prototype.sql = components.clientAction.namespaceFactory();
  const sql = Client.prototype.sql.prototype;

  sql.translateSQL = ca({
    url: {
      fmt: `${SQL_TRANSLATE_ROUTE}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.translatePPL = ca({
    url: {
      fmt: `${PPL_TRANSLATE_ROUTE}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.sqlQuery = ca({
    url: {
      fmt: `${SQL_QUERY_ROUTE}`,
    },
    needBody: true,
    method: 'POST',
  }); //default: jdbc

  sql.pplQuery = ca({
    url: {
      fmt: `${PPL_QUERY_ROUTE}`,
    },
    needBody: true,
    method: 'POST',
  }); //default: jdbc

  sql.sqlJson = ca({
    url: {
      fmt: `${SQL_QUERY_ROUTE}?${FORMAT_JSON}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.pplJson = ca({
    url: {
      fmt: `${PPL_QUERY_ROUTE}?${FORMAT_JSON}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.sqlCsv = ca({
    url: {
      fmt: `${SQL_QUERY_ROUTE}?${FORMAT_CSV}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.pplCsv = ca({
    url: {
      fmt: `${PPL_QUERY_ROUTE}?${FORMAT_CSV}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.sqlText = ca({
    url: {
      fmt: `${SQL_QUERY_ROUTE}?${FORMAT_TEXT}`,
    },
    needBody: true,
    method: 'POST',
  });

  sql.pplText = ca({
    url: {
      fmt: `${PPL_QUERY_ROUTE}?${FORMAT_TEXT}`,
    },
    needBody: true,
    method: 'POST',
  });
}
