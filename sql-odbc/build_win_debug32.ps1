# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

$WORKING_DIR = (Get-Location).Path
.\scripts\build_windows.ps1 $WORKING_DIR Debug 32
