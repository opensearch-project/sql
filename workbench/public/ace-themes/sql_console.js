/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import * as ace from 'brace';

ace.define('ace/theme/sql_console', ['require', 'exports', 'module', 'ace/lib/dom'], function (acequire, exports, module) {
  exports.isDark = false;
  exports.cssClass = 'ace-sql-console';
  exports.cssText = require('../index.scss');

  const dom = acequire('../lib/dom');
  dom.importCssString(exports.cssText, exports.cssClass);
});
