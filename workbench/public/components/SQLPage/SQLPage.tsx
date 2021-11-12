/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import React from "react";
import {
  EuiPanel,
  EuiButton,
  EuiFlexGroup,
  EuiFlexItem,
  EuiOverlayMask,
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiModalFooter,
  EuiCodeBlock,
  EuiText,
  EuiCodeEditor,
  EuiSpacer,
} from "@elastic/eui";
import { ResponseDetail, TranslateResult } from '../Main/main';
import _ from 'lodash';
import "brace/mode/sql";
import "../../ace-themes/sql_console";
import 'brace/ext/language_tools';

interface SQLPageProps {
  onRun: (query: string) => void,
  onTranslate: (query: string) => void,
  onClear: () => void,
  updateSQLQueries: (query: string) => void
  sqlQuery: string,
  sqlTranslations: ResponseDetail<TranslateResult>[]
}

interface SQLPageState {
  sqlQuery: string,
  translation: string,
  isModalVisible: boolean
}

export class SQLPage extends React.Component<SQLPageProps, SQLPageState> {
  constructor(props: SQLPageProps) {
    super(props);
    this.state = {
      sqlQuery: this.props.sqlQuery,
      translation: "",
      isModalVisible: false
    };
  }

  setIsModalVisible(visible: boolean): void {
    this.setState({
      isModalVisible: visible
    })
  }

  render() {

    const closeModal = () => this.setIsModalVisible(false);
    const showModal = () => this.setIsModalVisible(true);

    const sqlTranslationsNotEmpty = () => {
      if (this.props.sqlTranslations.length > 0) {
        return this.props.sqlTranslations[0].fulfilled;
      }
      return false;
    }

    const explainContent = sqlTranslationsNotEmpty()
    ? this.props.sqlTranslations.map((queryTranslation: any) => JSON.stringify(queryTranslation.data, null, 2)).join("\n")
    : 'This query is not explainable.';

    let modal;

    if (this.state.isModalVisible) {
      modal = (
        <EuiOverlayMask onClick={closeModal}>
          <EuiModal onClose={closeModal} style={{ width: 800 }}>
            <EuiModalHeader>
              <EuiModalHeaderTitle>Explain</EuiModalHeaderTitle>
            </EuiModalHeader>

            <EuiModalBody>
              <EuiCodeBlock
                language="json"
                fontSize="m"
                isCopyable
              >
                {explainContent}
              </EuiCodeBlock>
            </EuiModalBody>

            <EuiModalFooter>
              <EuiButton onClick={closeModal} fill>
                Close
            </EuiButton>
            </EuiModalFooter>
          </EuiModal>
        </EuiOverlayMask>
      );
    }

    return (
      <EuiPanel className="sql-console-query-editor container-panel" paddingSize="l">
        <EuiText className="sql-query-panel-header"><h3>Query editor</h3></EuiText>
        <EuiSpacer size="s" />
        <EuiCodeEditor
          mode="sql"
          theme="sql_console"
          width="100%"
          height="7rem"
          value={this.props.sqlQuery}
          onChange={this.props.updateSQLQueries}
          showPrintMargin={false}
          setOptions={{
            fontSize: "14px",
            enableBasicAutocompletion: true,
            enableLiveAutocompletion: true
          }}
          aria-label="Code Editor"
        />
        <EuiSpacer />
        <EuiFlexGroup className="action-container" gutterSize="m">
          <EuiFlexItem
            grow={false}
            onClick={() => this.props.onRun(this.props.sqlQuery)}
          >
            <EuiButton fill={true} className="sql-editor-button" >
              Run
            </EuiButton>
          </EuiFlexItem>
          <EuiFlexItem
            grow={false}
            onClick={() => {
              this.props.updateSQLQueries("");
              this.props.onClear();
            }}
          >
            <EuiButton className="sql-editor-button">
              Clear
            </EuiButton>
          </EuiFlexItem>
          <EuiFlexItem
            grow={false}
            onClick={() =>
              this.props.onTranslate(this.props.sqlQuery)
            }
          >
            <EuiButton
              className="sql-editor-button"
              onClick={showModal}
            >
              Explain
            </EuiButton>
            {modal}
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>
    )
  }
}
