/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


module.exports = {
  rootDir: "../",
  setupFiles: ["<rootDir>/test/polyfills.ts", "<rootDir>/test/setupTests.ts"],
  setupFilesAfterEnv: ["<rootDir>/test/setup.jest.ts"],
  roots: ["<rootDir>"],
  coverageDirectory: "./coverage",
  moduleNameMapper: {
    "\\.(css|less|scss)$": "<rootDir>/test/mocks/styleMock.ts",
    "^ui/(.*)": "<rootDir>/../../src/ui/public/$1/",
  },
  coverageReporters: ["lcov", "text", "cobertura"],
  testMatch: ["**/*.test.ts", "**/*.test.tsx"],
  collectCoverageFrom: [
    "!**/*.ts",
    "**/*.tsx",
    "!**/*.js",
    "!**/*.jsx",
    "!**/models/**",
    "!**/node_modules/**",
    "!**/index.ts",
    "!<rootDir>/index.js",
    "!<rootDir>/public/app.js",
    "!<rootDir>/public/temporary/**",
    "!<rootDir>/babel.config.js",
    "!<rootDir>/test/**",
    "!<rootDir>/server/**",
    "!<rootDir>/coverage/**",
    "!<rootDir>/scripts/**",
    "!<rootDir>/build/**",
    "!**/vendor/**",
  ],
  clearMocks: true,
  testPathIgnorePatterns: ["<rootDir>/build/", "<rootDir>/node_modules/"],
  testEnvironment: 'jsdom',
};
