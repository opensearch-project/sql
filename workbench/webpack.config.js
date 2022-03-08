/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


const path = require('path');
import './public/ace-themes/sql_console.css';

module.exports = {
  debug: true,
  mode: 'development',
  entry: 'public/ace-themes/sql_console.css',
  resolve: {
    extensions: ['', '.js', '.jsx', '.ts', '.tsx']
  },
  module: {
    rules: [
      {
        test: /\.css$/,
        use: [
          'style-loader',
          {
            loader: 'css-loader',
            options: {
              importLoaders: 1,
              modules: true
            }
          }
        ],
        include: /\.module\.css$/
      },
      {
        test: /\.css$/,
        use: [
          'style-loader',
          'css-loader'
        ],
        exclude: /\.module\.css$/
      }
    ]
  }
}
