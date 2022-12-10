/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import React from "react";
import "@testing-library/jest-dom/extend-expect";
import { render } from "@testing-library/react";
import Switch from "./Switch";


const onChange = jest.fn();

describe("<Switch /> spec", () => {
  it("renders the component", () => {
    render(<Switch onChange={onChange} language="SQL" />);
    expect(document.body.children[0]).toMatchSnapshot();
  });
});
