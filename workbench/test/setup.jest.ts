/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import "@testing-library/jest-dom/extend-expect";
import 'mutationobserver-shim';
import { configure } from "@testing-library/react";

configure({ testIdAttribute: "data-test-subj" });

jest.mock("@elastic/eui/lib/components/form/form_row/make_id", () => () => "some_make_id");

jest.mock("@elastic/eui/lib/services/accessibility/html_id_generator", () => ({
  htmlIdGenerator: () => {
    return () => "some_html_id";
  },
}));

// @ts-ignore
window.Worker = function() {
  this.postMessage = () => {};
  // @ts-ignore
  this.terminate = () => {};
};

// @ts-ignore
window.URL = {
  createObjectURL: () => {
    return "";
  },
};

jest.mock("@elastic/eui/lib/components/icon", () => ({
  EuiIcon: () => "EuiIconMock",
  __esModule: true,
  IconPropType: require("@elastic/eui/lib/components/icon/icon").IconPropType,
  ICON_TYPES: require("@elastic/eui/lib/components/icon/icon").TYPES,
  ICON_SIZES: require("@elastic/eui/lib/components/icon/icon").SIZES,
  ICON_COLORS: require("@elastic/eui/lib/components/icon/icon").COLORS,
}));
