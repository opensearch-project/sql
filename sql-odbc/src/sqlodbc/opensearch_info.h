#ifndef __OPENSEARCH_INFO_H__
#define __OPENSEARCH_INFO_H__
#include "opensearch_helper.h"
#include "opensearch_odbc.h"
#include "unicode_support.h"

#ifndef WIN32
#include <ctype.h>
#endif

#include "bind.h"
#include "catfunc.h"
#include "dlg_specific.h"
#include "environ.h"
#include "misc.h"
#include "multibyte.h"
#include "opensearch_apifunc.h"
#include "opensearch_connection.h"
#include "opensearch_types.h"
#include "qresult.h"
#include "statement.h"
#include "tuple.h"

// C Interface
#ifdef __cplusplus
extern "C" {
#endif
RETCODE SQL_API OPENSEARCHAPI_Tables(HSTMT hstmt, const SQLCHAR* catalog_name_sql,
                             const SQLSMALLINT catalog_name_sz,
                             const SQLCHAR* schema_name_sql,
                             const SQLSMALLINT schema_name_sz,
                             const SQLCHAR* table_name_sql,
                             const SQLSMALLINT table_name_sz,
                             const SQLCHAR* table_type_sql,
                             const SQLSMALLINT table_type_sz, const UWORD flag);
RETCODE SQL_API
OPENSEARCHAPI_Columns(HSTMT hstmt, const SQLCHAR* catalog_name_sql,
              const SQLSMALLINT catalog_name_sz, const SQLCHAR* schema_name_sql,
              const SQLSMALLINT schema_name_sz, const SQLCHAR* table_name_sql,
              const SQLSMALLINT table_name_sz, const SQLCHAR* column_name_sql,
              const SQLSMALLINT column_name_sz, const UWORD flag,
              const OID reloid, const Int2 attnum);

RETCODE SQL_API OPENSEARCHAPI_GetTypeInfo(HSTMT hstmt, SQLSMALLINT fSqlType);
#ifdef __cplusplus
}
#endif

#endif /* __OPENSEARCH_INFO_H__ */
