/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import QueryService from "../../server/services/QueryService";
import TranslateService from "../../server/services/TranslateService";
import httpClientMock from "./httpClientMock";

const queryService = new QueryService(httpClientMock);
const translateService = new TranslateService(httpClientMock);

export default { queryService, translateService };
