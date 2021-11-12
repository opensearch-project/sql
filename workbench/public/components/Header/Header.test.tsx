/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import React from "react";
import "@testing-library/jest-dom/extend-expect";
import { render } from "@testing-library/react";
import Header from "./Header";


describe("<Header /> spec", () => {
  it("renders the component", () => {
    render(<Header />);
    expect(document.body.children[0]).toMatchSnapshot();
  });
});
