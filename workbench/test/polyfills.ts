/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


// @ts-ignore
import { MutationObserver } from "./polyfills/mutationObserver";

Object.defineProperty(window, "MutationObserver", { value: MutationObserver });
