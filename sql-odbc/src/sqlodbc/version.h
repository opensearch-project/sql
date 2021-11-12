/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

#ifndef __VERSION_H__
#define __VERSION_H__

/*
 *	BuildAll may pass ELASTICDRIVERVERSION, ELASTIC_RESOURCE_VERSION
 *	and OPENSEARCH_DRVFILE_VERSION via winbuild/elasticodbc.vcxproj.
 */
#ifdef OPENSEARCH_ODBC_VERSION

#ifndef OPENSEARCHDRIVERVERSION
#define OPENSEARCHDRIVERVERSION OPENSEARCH_ODBC_VERSION
#endif
#ifndef OPENSEARCH_RESOURCE_VERSION
#define OPENSEARCH_RESOURCE_VERSION OPENSEARCHDRIVERVERSION
#endif
#ifndef OPENSEARCH_DRVFILE_VERSION
#define OPENSEARCH_DRVFILE_VERSION OPENSEARCH_ODBC_DRVFILE_VERSION
#endif

#endif  // OPENSEARCH_ODBC_VERSION

#endif
