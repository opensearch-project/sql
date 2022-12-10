/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


const httpClientMock = jest.fn() as any;

httpClientMock.delete = jest.fn();
httpClientMock.get = jest.fn();
httpClientMock.head = jest.fn();
httpClientMock.post = jest.fn();
httpClientMock.put = jest.fn();

export default httpClientMock;
