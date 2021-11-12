/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import React from 'react'
import { EuiPanel } from '@elastic/eui';

export function PanelWrapper({ shouldWrap, children }: { shouldWrap: boolean; children: React.ReactNode }) {
  return shouldWrap ?
    <div style={{ backgroundColor: "#f5f7fa", padding: 25 }}>
      <EuiPanel paddingSize="none">{children}</EuiPanel>
    </div> : <>{children}</>;
}
