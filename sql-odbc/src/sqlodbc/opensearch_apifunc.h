/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


#ifndef _OPENSEARCH_API_FUNC_H__
#define _OPENSEARCH_API_FUNC_H__

#include <stdio.h>
#include <string.h>

#include "opensearch_odbc.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/*	Internal flags for catalog functions */
#define PODBC_NOT_SEARCH_PATTERN 1L
#define PODBC_SEARCH_PUBLIC_SCHEMA (1L << 1)
#define PODBC_SEARCH_BY_IDS (1L << 2)
#define PODBC_SHOW_OID_COLUMN (1L << 3)
#define PODBC_ROW_VERSIONING (1L << 4)
/*	Internal flags for OPENSEARCHAPI_AllocStmt functions */
#define PODBC_EXTERNAL_STATEMENT 1L /* visible to the driver manager */
#define PODBC_INHERIT_CONNECT_OPTIONS (1L << 1)
/*	Internal flags for OPENSEARCHAPI_Exec... functions */
/*	Flags for the error handling */
#define PODBC_ALLOW_PARTIAL_EXTRACT 1L
/* #define	PODBC_ERROR_CLEAR		(1L << 1) 	no longer used */

RETCODE SQL_API OPENSEARCHAPI_AllocConnect(HENV EnvironmentHandle,
                                   HDBC *ConnectionHandle);
RETCODE SQL_API OPENSEARCHAPI_AllocEnv(HENV *EnvironmentHandle);
RETCODE SQL_API OPENSEARCHAPI_AllocStmt(HDBC ConnectionHandle, HSTMT *StatementHandle,
                                UDWORD flag);
RETCODE SQL_API OPENSEARCHAPI_BindCol(HSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                              SQLSMALLINT TargetType, PTR TargetValue,
                              SQLLEN BufferLength, SQLLEN *StrLen_or_Ind);
RETCODE SQL_API OPENSEARCHAPI_Connect(HDBC ConnectionHandle, const SQLCHAR *ServerName,
                              SQLSMALLINT NameLength1, const SQLCHAR *UserName,
                              SQLSMALLINT NameLength2,
                              const SQLCHAR *Authentication,
                              SQLSMALLINT NameLength3);
RETCODE SQL_API OPENSEARCHAPI_BrowseConnect(HDBC hdbc, const SQLCHAR *szConnStrIn,
                                    SQLSMALLINT cbConnStrIn,
                                    SQLCHAR *szConnStrOut,
                                    SQLSMALLINT cbConnStrOutMax,
                                    SQLSMALLINT *pcbConnStrOut);
RETCODE SQL_API OPENSEARCHAPI_DescribeCol(
    HSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
    SQLSMALLINT BufferLength, SQLSMALLINT *NameLength, SQLSMALLINT *DataType,
    SQLULEN *ColumnSize, SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable);
RETCODE SQL_API OPENSEARCHAPI_Disconnect(HDBC ConnectionHandle);
/* Helper functions for Error handling */
RETCODE SQL_API OPENSEARCHAPI_EnvError(HENV EnvironmentHandle, SQLSMALLINT RecNumber,
                               SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
                               SQLCHAR *MessageText, SQLSMALLINT BufferLength,
                               SQLSMALLINT *TextLength, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_ConnectError(HDBC ConnectionHandle, SQLSMALLINT RecNumber,
                                   SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
                                   SQLCHAR *MessageText,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT *TextLength, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_StmtError(HSTMT StatementHandle, SQLSMALLINT RecNumber,
                                SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
                                SQLCHAR *MessageText, SQLSMALLINT BufferLength,
                                SQLSMALLINT *TextLength, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_ExecDirect(HSTMT StatementHandle,
                                 const SQLCHAR *StatementText,
                                 SQLINTEGER TextLength, BOOL commit);
RETCODE SQL_API OPENSEARCHAPI_Execute(HSTMT StatementHandle);
RETCODE SQL_API OPENSEARCHAPI_Fetch(HSTMT StatementHandle);
RETCODE SQL_API OPENSEARCHAPI_FreeConnect(HDBC ConnectionHandle);
RETCODE SQL_API OPENSEARCHAPI_FreeEnv(HENV EnvironmentHandle);
RETCODE SQL_API OPENSEARCHAPI_FreeStmt(HSTMT StatementHandle, SQLUSMALLINT Option);
RETCODE SQL_API OPENSEARCHAPI_GetConnectOption(HDBC ConnectionHandle,
                                       SQLUSMALLINT Option, PTR Value,
                                       SQLINTEGER *StringLength,
                                       SQLINTEGER BufferLength);
RETCODE SQL_API OPENSEARCHAPI_GetCursorName(HSTMT StatementHandle, SQLCHAR *CursorName,
                                    SQLSMALLINT BufferLength,
                                    SQLSMALLINT *NameLength);
RETCODE SQL_API OPENSEARCHAPI_GetData(HSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                              SQLSMALLINT TargetType, PTR TargetValue,
                              SQLLEN BufferLength, SQLLEN *StrLen_or_Ind);
RETCODE SQL_API OPENSEARCHAPI_GetFunctions(HDBC ConnectionHandle,
                                   SQLUSMALLINT FunctionId,
                                   SQLUSMALLINT *Supported);
RETCODE SQL_API OPENSEARCHAPI_GetFunctions30(HDBC ConnectionHandle,
                                     SQLUSMALLINT FunctionId,
                                     SQLUSMALLINT *Supported);
RETCODE SQL_API OPENSEARCHAPI_GetInfo(HDBC ConnectionHandle, SQLUSMALLINT InfoType,
                              PTR InfoValue, SQLSMALLINT BufferLength,
                              SQLSMALLINT *StringLength);
RETCODE SQL_API OPENSEARCHAPI_GetStmtOption(HSTMT StatementHandle, SQLUSMALLINT Option,
                                    PTR Value, SQLINTEGER *StringLength,
                                    SQLINTEGER BufferLength);
RETCODE SQL_API OPENSEARCHAPI_NumResultCols(HSTMT StatementHandle,
                                    SQLSMALLINT *ColumnCount);
RETCODE SQL_API OPENSEARCHAPI_RowCount(HSTMT StatementHandle, SQLLEN *RowCount);
RETCODE SQL_API OPENSEARCHAPI_SetConnectOption(HDBC ConnectionHandle,
                                       SQLUSMALLINT Option, SQLULEN Value);
RETCODE SQL_API OPENSEARCHAPI_SetCursorName(HSTMT StatementHandle,
                                    const SQLCHAR *CursorName,
                                    SQLSMALLINT NameLength);
RETCODE SQL_API OPENSEARCHAPI_SetStmtOption(HSTMT StatementHandle, SQLUSMALLINT Option,
                                    SQLULEN Value);
RETCODE SQL_API
OPENSEARCHAPI_SpecialColumns(HSTMT StatementHandle, SQLUSMALLINT IdentifierType,
                     const SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                     const SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                     const SQLCHAR *TableName, SQLSMALLINT NameLength3,
                     SQLUSMALLINT Scope, SQLUSMALLINT Nullable);
RETCODE SQL_API OPENSEARCHAPI_Statistics(
    HSTMT StatementHandle, const SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    const SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    const SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLUSMALLINT Unique,
    SQLUSMALLINT Reserved);
RETCODE SQL_API OPENSEARCHAPI_ColAttributes(HSTMT hstmt, SQLUSMALLINT icol,
                                    SQLUSMALLINT fDescType, PTR rgbDesc,
                                    SQLSMALLINT cbDescMax, SQLSMALLINT *pcbDesc,
                                    SQLLEN *pfDesc);
RETCODE SQL_API OPENSEARCHAPI_Prepare(HSTMT hstmt, const SQLCHAR *szSqlStr,
                              SQLINTEGER cbSqlStr);
RETCODE SQL_API OPENSEARCHAPI_ColumnPrivileges(
    HSTMT hstmt, const SQLCHAR *szCatalogName, SQLSMALLINT cbCatalogName,
    const SQLCHAR *szSchemaName, SQLSMALLINT cbSchemaName,
    const SQLCHAR *szTableName, SQLSMALLINT cbTableName,
    const SQLCHAR *szColumnName, SQLSMALLINT cbColumnName, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_ExtendedFetch(HSTMT hstmt, SQLUSMALLINT fFetchType,
                                    SQLLEN irow, SQLULEN *pcrow,
                                    SQLUSMALLINT *rgfRowStatus,
                                    SQLLEN FetchOffset, SQLLEN rowsetSize);
RETCODE SQL_API OPENSEARCHAPI_ForeignKeys(
    HSTMT hstmt, const SQLCHAR *szPkCatalogName, SQLSMALLINT cbPkCatalogName,
    const SQLCHAR *szPkSchemaName, SQLSMALLINT cbPkSchemaName,
    const SQLCHAR *szPkTableName, SQLSMALLINT cbPkTableName,
    const SQLCHAR *szFkCatalogName, SQLSMALLINT cbFkCatalogName,
    const SQLCHAR *szFkSchemaName, SQLSMALLINT cbFkSchemaName,
    const SQLCHAR *szFkTableName, SQLSMALLINT cbFkTableName);
RETCODE SQL_API OPENSEARCHAPI_MoreResults(HSTMT hstmt);
RETCODE SQL_API OPENSEARCHAPI_NativeSql(HDBC hdbc, const SQLCHAR *szSqlStrIn,
                                SQLINTEGER cbSqlStrIn, SQLCHAR *szSqlStr,
                                SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr);
RETCODE SQL_API OPENSEARCHAPI_NumParams(HSTMT hstmt, SQLSMALLINT *pcpar);
RETCODE SQL_API OPENSEARCHAPI_PrimaryKeys(HSTMT hstmt, const SQLCHAR *szCatalogName,
                                  SQLSMALLINT cbCatalogName,
                                  const SQLCHAR *szSchemaName,
                                  SQLSMALLINT cbSchemaName,
                                  const SQLCHAR *szTableName,
                                  SQLSMALLINT cbTableName, OID reloid);
RETCODE SQL_API OPENSEARCHAPI_ProcedureColumns(
    HSTMT hstmt, const SQLCHAR *szCatalogName, SQLSMALLINT cbCatalogName,
    const SQLCHAR *szSchemaName, SQLSMALLINT cbSchemaName,
    const SQLCHAR *szProcName, SQLSMALLINT cbProcName,
    const SQLCHAR *szColumnName, SQLSMALLINT cbColumnName, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_Procedures(HSTMT hstmt, const SQLCHAR *szCatalogName,
                                 SQLSMALLINT cbCatalogName,
                                 const SQLCHAR *szSchemaName,
                                 SQLSMALLINT cbSchemaName,
                                 const SQLCHAR *szProcName,
                                 SQLSMALLINT cbProcName, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_TablePrivileges(HSTMT hstmt, const SQLCHAR *szCatalogName,
                                      SQLSMALLINT cbCatalogName,
                                      const SQLCHAR *szSchemaName,
                                      SQLSMALLINT cbSchemaName,
                                      const SQLCHAR *szTableName,
                                      SQLSMALLINT cbTableName, UWORD flag);
RETCODE SQL_API OPENSEARCHAPI_GetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                 SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                 SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                 SQLSMALLINT BufferLength,
                                 SQLSMALLINT *TextLength);
RETCODE SQL_API OPENSEARCHAPI_GetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber,
                                   SQLSMALLINT DiagIdentifier, PTR DiagInfoPtr,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLengthPtr);
RETCODE SQL_API OPENSEARCHAPI_GetConnectAttr(HDBC ConnectionHandle,
                                     SQLINTEGER Attribute, PTR Value,
                                     SQLINTEGER BufferLength,
                                     SQLINTEGER *StringLength);
RETCODE SQL_API OPENSEARCHAPI_GetStmtAttr(HSTMT StatementHandle, SQLINTEGER Attribute,
                                  PTR Value, SQLINTEGER BufferLength,
                                  SQLINTEGER *StringLength);

/* Driver-specific connection attributes, for SQLSet/GetConnectAttr() */
enum {
    SQL_ATTR_ESOPT_DEBUG = 65536,
    SQL_ATTR_ESOPT_COMMLOG = 65537,
    SQL_ATTR_ESOPT_PARSE = 65538,
    SQL_ATTR_ESOPT_USE_DECLAREFETCH = 65539,
    SQL_ATTR_ESOPT_SERVER_SIDE_PREPARE = 65540,
    SQL_ATTR_ESOPT_FETCH = 65541,
    SQL_ATTR_ESOPT_UNKNOWNSIZES = 65542,
    SQL_ATTR_ESOPT_TEXTASLONGVARCHAR = 65543,
    SQL_ATTR_ESOPT_UNKNOWNSASLONGVARCHAR = 65544,
    SQL_ATTR_ESOPT_BOOLSASCHAR = 65545,
    SQL_ATTR_ESOPT_MAXVARCHARSIZE = 65546,
    SQL_ATTR_ESOPT_MAXLONGVARCHARSIZE = 65547,
    SQL_ATTR_ESOPT_WCSDEBUG = 65548,
    SQL_ATTR_ESOPT_MSJET = 65549
};
RETCODE SQL_API OPENSEARCHAPI_SetConnectAttr(HDBC ConnectionHandle,
                                     SQLINTEGER Attribute, PTR Value,
                                     SQLINTEGER StringLength);
RETCODE SQL_API OPENSEARCHAPI_SetStmtAttr(HSTMT StatementHandle, SQLINTEGER Attribute,
                                  PTR Value, SQLINTEGER StringLength);
RETCODE SQL_API OPENSEARCHAPI_AllocDesc(HDBC ConnectionHandle,
                                SQLHDESC *DescriptorHandle);
RETCODE SQL_API OPENSEARCHAPI_FreeDesc(SQLHDESC DescriptorHandle);
RETCODE SQL_API OPENSEARCHAPI_CopyDesc(SQLHDESC SourceDescHandle,
                               SQLHDESC TargetDescHandle);
RETCODE SQL_API OPENSEARCHAPI_SetDescField(SQLHDESC DescriptorHandle,
                                   SQLSMALLINT RecNumber,
                                   SQLSMALLINT FieldIdentifier, PTR Value,
                                   SQLINTEGER BufferLength);
RETCODE SQL_API OPENSEARCHAPI_GetDescField(SQLHDESC DescriptorHandle,
                                   SQLSMALLINT RecNumber,
                                   SQLSMALLINT FieldIdentifier, PTR Value,
                                   SQLINTEGER BufferLength,
                                   SQLINTEGER *StringLength);
RETCODE SQL_API OPENSEARCHAPI_DescError(SQLHDESC DescriptorHandle,
                                SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                SQLSMALLINT BufferLength,
                                SQLSMALLINT *TextLength, UWORD flag);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* define_OPENSEARCH_API_FUNC_H__ */
