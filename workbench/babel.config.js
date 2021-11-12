/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


// babelrc doesn't respect NODE_PATH anymore but using require does.
// Alternative to install them locally in node_modules
module.exports = {
  presets: [require("@babel/preset-env"), require("@babel/preset-react"), require("@babel/preset-typescript")],
  plugins: [require("@babel/plugin-proposal-class-properties"), require("@babel/plugin-proposal-object-rest-spread"), ["@babel/transform-runtime"]]
};
