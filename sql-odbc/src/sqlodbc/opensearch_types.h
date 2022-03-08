#ifndef OPENSEARCH_TYPES
#define OPENSEARCH_TYPES

#include "dlg_specific.h"
#include "opensearch_odbc.h"
#ifdef __cplusplus
extern "C" {
#endif

/* the type numbers are defined by the OID's of the types' rows */
/* in table opensearch_type */

#ifdef NOT_USED
#define ES_TYPE_LO ? ? ? ? /* waiting for permanent type */
#endif

#define OPENSEARCH_TYPE_NAME_BOOLEAN "boolean"
#define OPENSEARCH_TYPE_NAME_BYTE "byte"
#define OPENSEARCH_TYPE_NAME_SHORT "short"
#define OPENSEARCH_TYPE_NAME_INTEGER "integer"
#define OPENSEARCH_TYPE_NAME_LONG "long"
#define OPENSEARCH_TYPE_NAME_HALF_FLOAT "half_float"
#define OPENSEARCH_TYPE_NAME_FLOAT "float"
#define OPENSEARCH_TYPE_NAME_DOUBLE "double"
#define OPENSEARCH_TYPE_NAME_SCALED_FLOAT "scaled_float"
#define OPENSEARCH_TYPE_NAME_KEYWORD "keyword"
#define OPENSEARCH_TYPE_NAME_TEXT "text"
#define OPENSEARCH_TYPE_NAME_NESTED "nested"
#define OPENSEARCH_TYPE_NAME_DATE "date"
#define OPENSEARCH_TYPE_NAME_TIMESTAMP "timestamp"
#define OPENSEARCH_TYPE_NAME_OBJECT "object"
#define OPENSEARCH_TYPE_NAME_VARCHAR "varchar"
#define OPENSEARCH_TYPE_NAME_UNSUPPORTED "unsupported"

#define MS_ACCESS_SERIAL "int identity"
#define OPENSEARCH_TYPE_BOOL 16
#define OPENSEARCH_TYPE_BYTEA 17
#define OPENSEARCH_TYPE_CHAR 18
#define OPENSEARCH_TYPE_NAME 19
#define OPENSEARCH_TYPE_INT8 20
#define OPENSEARCH_TYPE_INT2 21
#define OPENSEARCH_TYPE_INT2VECTOR 22
#define OPENSEARCH_TYPE_INT4 23
#define OPENSEARCH_TYPE_REGPROC 24
#define OPENSEARCH_TYPE_TEXT 25
#define OPENSEARCH_TYPE_OID 26
#define OPENSEARCH_TYPE_TID 27
#define OPENSEARCH_TYPE_XID 28
#define OPENSEARCH_TYPE_CID 29
#define OPENSEARCH_TYPE_OIDVECTOR 30
#define OPENSEARCH_TYPE_INT1 31
#define OPENSEARCH_TYPE_HALF_FLOAT 32
#define OPENSEARCH_TYPE_SCALED_FLOAT 33
#define OPENSEARCH_TYPE_KEYWORD 34
#define OPENSEARCH_TYPE_NESTED 35
#define OPENSEARCH_TYPE_OBJECT 36
#define OPENSEARCH_TYPE_XML 142
#define OPENSEARCH_TYPE_XMLARRAY 143
#define OPENSEARCH_TYPE_CIDR 650
#define OPENSEARCH_TYPE_FLOAT4 700
#define OPENSEARCH_TYPE_FLOAT8 701
#define OPENSEARCH_TYPE_ABSTIME 702
#define OPENSEARCH_TYPE_UNKNOWN 705
#define OPENSEARCH_TYPE_MONEY 790
#define OPENSEARCH_TYPE_MACADDR 829
#define OPENSEARCH_TYPE_INET 869
#define OPENSEARCH_TYPE_TEXTARRAY 1009
#define OPENSEARCH_TYPE_BPCHARARRAY 1014
#define OPENSEARCH_TYPE_VARCHARARRAY 1015
#define OPENSEARCH_TYPE_BPCHAR 1042
#define OPENSEARCH_TYPE_VARCHAR 1043
#define OPENSEARCH_TYPE_DATE 1082
#define OPENSEARCH_TYPE_TIME 1083
#define OPENSEARCH_TYPE_TIMESTAMP_NO_TMZONE 1114 /* since 7.2 */
#define OPENSEARCH_TYPE_DATETIME 1184            /* timestamptz */
#define OPENSEARCH_TYPE_INTERVAL 1186
#define OPENSEARCH_TYPE_TIME_WITH_TMZONE 1266 /* since 7.1 */
#define OPENSEARCH_TYPE_TIMESTAMP 1296        /* deprecated since 7.0 */
#define OPENSEARCH_TYPE_BIT 1560
#define OPENSEARCH_TYPE_NUMERIC 1700
#define OPENSEARCH_TYPE_REFCURSOR 1790
#define OPENSEARCH_TYPE_RECORD 2249
#define OPENSEARCH_TYPE_ANY 2276
#define OPENSEARCH_TYPE_VOID 2278
#define OPENSEARCH_TYPE_UUID 2950
#define INTERNAL_ASIS_TYPE (-9999)

#define TYPE_MAY_BE_ARRAY(type) \
    ((type) == OPENSEARCH_TYPE_XMLARRAY || ((type) >= 1000 && (type) <= 1041))
/* extern Int4 opensearch_types_defined[]; */
extern SQLSMALLINT sqlTypes[];

/*	Defines for opensearchtype_precision */
#define OPENSEARCH_ATP_UNSET (-3)   /* atttypmod */
#define OPENSEARCH_ADT_UNSET (-3)   /* adtsize_or_longestlen */
#define OPENSEARCH_UNKNOWNS_UNSET 0 /* UNKNOWNS_AS_MAX */
#define OPENSEARCH_WIDTH_OF_BOOLS_AS_CHAR 5

/*
 *	SQL_INTERVAL support is disabled because I found
 *	some applications which are unhappy with it.
 *
#define	OPENSEARCH_INTERVAL_AS_SQL_INTERVAL
 */

OID opensearch_true_type(const ConnectionClass *, OID, OID);
OID sqltype_to_opensearchtype(const ConnectionClass *conn, SQLSMALLINT fSqlType);
const char *sqltype_to_opensearchcast(const ConnectionClass *conn,
                              SQLSMALLINT fSqlType);

SQLSMALLINT opensearchtype_to_concise_type(const StatementClass *stmt, OID type,
                                   int col, int handle_unknown_size_as);
SQLSMALLINT opensearchtype_to_sqldesctype(const StatementClass *stmt, OID type, int col,
                                  int handle_unknown_size_as);
const char *opensearchtype_to_name(const StatementClass *stmt, OID type, int col,
                           BOOL auto_increment);

SQLSMALLINT opensearchtype_attr_to_concise_type(const ConnectionClass *conn, OID type,
                                        int typmod, int adtsize_or_longestlen,
                                        int handle_unknown_size_as);
SQLSMALLINT opensearchtype_attr_to_sqldesctype(const ConnectionClass *conn, OID type,
                                       int typmod, int adtsize_or_longestlen,
                                       int handle_unknown_size_as);
SQLSMALLINT opensearchtype_attr_to_datetime_sub(const ConnectionClass *conn, OID type,
                                        int typmod);
SQLSMALLINT opensearchtype_attr_to_ctype(const ConnectionClass *conn, OID type,
                                 int typmod);
const char *opensearchtype_attr_to_name(const ConnectionClass *conn, OID type,
                                int typmod, BOOL auto_increment);
Int4 opensearchtype_attr_column_size(const ConnectionClass *conn, OID type,
                             int atttypmod, int adtsize_or_longest,
                             int handle_unknown_size_as);
Int4 opensearchtype_attr_buffer_length(const ConnectionClass *conn, OID type,
                               int atttypmod, int adtsize_or_longestlen,
                               int handle_unknown_size_as);
Int4 opensearchtype_attr_display_size(const ConnectionClass *conn, OID type,
                              int atttypmod, int adtsize_or_longestlen,
                              int handle_unknown_size_as);
Int2 opensearchtype_attr_decimal_digits(const ConnectionClass *conn, OID type,
                                int atttypmod, int adtsize_or_longestlen,
                                int handle_unknown_size_as);
Int4 opensearchtype_attr_transfer_octet_length(const ConnectionClass *conn, OID type,
                                       int atttypmod,
                                       int handle_unknown_size_as);
SQLSMALLINT opensearchtype_attr_precision(const ConnectionClass *conn, OID type,
                                  int atttypmod, int adtsize_or_longest,
                                  int handle_unknown_size_as);
Int4 opensearchtype_attr_desclength(const ConnectionClass *conn, OID type,
                            int atttypmod, int adtsize_or_longestlen,
                            int handle_unknown_size_as);
Int2 opensearchtype_attr_scale(const ConnectionClass *conn, OID type, int atttypmod,
                       int adtsize_or_longestlen, int handle_unknown_size_as);

/*	These functions can use static numbers or result sets(col parameter) */
Int4 opensearchtype_column_size(
    const StatementClass *stmt, OID type, int col,
    int handle_unknown_size_as); /* corresponds to "precision" in ODBC 2.x */
SQLSMALLINT opensearchtype_precision(
    const StatementClass *stmt, OID type, int col,
    int handle_unknown_size_as); /* "precsion in ODBC 3.x */
/* the following size/length are of Int4 due to ES restriction */
Int4 opensearchtype_display_size(const StatementClass *stmt, OID type, int col,
                         int handle_unknown_size_as);
Int4 opensearchtype_buffer_length(const StatementClass *stmt, OID type, int col,
                          int handle_unknown_size_as);
Int4 opensearchtype_desclength(const StatementClass *stmt, OID type, int col,
                       int handle_unknown_size_as);
// Int4		opensearchtype_transfer_octet_length(const ConnectionClass *conn, OID type,
// int column_size);

SQLSMALLINT opensearchtype_decimal_digits(
    const StatementClass *stmt, OID type,
    int col); /* corresponds to "scale" in ODBC 2.x */
SQLSMALLINT opensearchtype_min_decimal_digits(
    const ConnectionClass *conn,
    OID type); /* corresponds to "min_scale" in ODBC 2.x */
SQLSMALLINT opensearchtype_max_decimal_digits(
    const ConnectionClass *conn,
    OID type); /* corresponds to "max_scale" in ODBC 2.x */
SQLSMALLINT opensearchtype_scale(const StatementClass *stmt, OID type,
                         int col); /* ODBC 3.x " */
Int2 opensearchtype_radix(const ConnectionClass *conn, OID type);
Int2 opensearchtype_nullable(const ConnectionClass *conn, OID type);
Int2 opensearchtype_auto_increment(const ConnectionClass *conn, OID type);
Int2 opensearchtype_case_sensitive(const ConnectionClass *conn, OID type);
Int2 opensearchtype_money(const ConnectionClass *conn, OID type);
Int2 opensearchtype_searchable(const ConnectionClass *conn, OID type);
Int2 opensearchtype_unsigned(const ConnectionClass *conn, OID type);
const char *opensearchtype_literal_prefix(const ConnectionClass *conn, OID type);
const char *opensearchtype_literal_suffix(const ConnectionClass *conn, OID type);
const char *opensearchtype_create_params(const ConnectionClass *conn, OID type);

SQLSMALLINT sqltype_to_default_ctype(const ConnectionClass *stmt,
                                     SQLSMALLINT sqltype);
Int4 ctype_length(SQLSMALLINT ctype);

SQLSMALLINT ansi_to_wtype(const ConnectionClass *self, SQLSMALLINT ansitype);

#ifdef __cplusplus
}
#endif

typedef enum {
    CONNECTION_OK,
    CONNECTION_BAD,
    /* Non-blocking mode only below here */

    /*
     * The existence of these should never be relied upon - they should only
     * be used for user feedback or similar purposes.
     */
    CONNECTION_STARTED,           /* Waiting for connection to be made.  */
    CONNECTION_MADE,              /* Connection OK; waiting to send.     */
    CONNECTION_AWAITING_RESPONSE, /* Waiting for a response from the postmaster.
                                   */
    CONNECTION_AUTH_OK, /* Received authentication; waiting for backend startup.
                         */
    CONNECTION_SETENV,  /* Negotiating environment. */
    CONNECTION_SSL_STARTUP,    /* Negotiating SSL. */
    CONNECTION_NEEDED,         /* Internal state: connect() needed */
    CONNECTION_CHECK_WRITABLE, /* Check if we could make a writable connection.
                                */
    CONNECTION_CONSUME,    /* Wait for any pending message and consume them. */
    CONNECTION_GSS_STARTUP /* Negotiating GSSAPI. */
} ConnStatusType;

typedef enum {
    CONN_ERROR_SUCCESS,             // 0
    CONN_ERROR_QUERY_SYNTAX,        // 42000
    CONN_ERROR_COMM_LINK_FAILURE,   // 08S01
    CONN_ERROR_INVALID_NULL_PTR,    // HY009
    CONN_ERROR_INVALID_AUTH,        // 28000
    CONN_ERROR_UNABLE_TO_ESTABLISH  // 08001
} ConnErrorType;

// Only expose this to C++ code, this will be passed through the C interface as
// a void*
#ifdef __cplusplus
#include <stdint.h>

#ifdef __APPLE__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif  // __APPLE__
#include "rabbit.hpp"
#ifdef __APPLE__
#pragma clang diagnostic pop
#endif  // __APPLE__

#include <string>
#include <vector>

typedef struct authentication_options {
    std::string auth_type;
    std::string username;
    std::string password;
    std::string region;
} authentication_options;

typedef struct encryption_options {
    bool use_ssl;
    bool verify_server;
    std::string certificate_type;
    std::string certificate;
    std::string key;
    std::string key_pw;
} encryption_options;

typedef struct connection_options {
    std::string server;
    std::string port;
    std::string timeout;
    std::string fetch_size;
} connection_options;

typedef struct runtime_options {
    connection_options conn;
    authentication_options auth;
    encryption_options crypt;
} runtime_options;

typedef struct ErrorDetails {
    std::string reason;
    std::string details;
    std::string source_type;
    ConnErrorType type;
    ErrorDetails() {
        reason = "";
        details = "";
        source_type = "";
        type = ConnErrorType::CONN_ERROR_SUCCESS;
    }
} ErrorDetails;

#define INVALID_OID 0
#define KEYWORD_TYPE_OID 1043
#define KEYWORD_TYPE_SIZE 255
#define KEYWORD_DISPLAY_SIZE 255
#define KEYWORD_LENGTH_OF_STR 255

// Copied from ColumnInfoClass's 'srvr_info' struct. Comments are the relevant
// name in 'srvr_info'
typedef struct ColumnInfo {
    std::string field_name;    // name
    uint32_t type_oid;         // adtid
    int16_t type_size;         // adtsize
    int32_t display_size;      // longest row
    int32_t length_of_str;     // the length of bpchar/varchar
    uint32_t relation_id;      // relid
    int16_t attribute_number;  // attid
    ColumnInfo() {
        field_name = "";
        type_oid = INVALID_OID;
        type_size = 0;      // ?
        display_size = 0;   // ?
        length_of_str = 0;  // ?
        relation_id = INVALID_OID;
        attribute_number = INVALID_OID;
    }
} ColumnInfo;

typedef struct OpenSearchResult {
    uint32_t ref_count;  // reference count. A ColumnInfo can be shared by
                         // several qresults.
    uint16_t num_fields;
    std::vector< ColumnInfo > column_info;
    std::string cursor;
    std::string result_json;
    std::string command_type;  // SELECT / FETCH / etc
    rabbit::document opensearch_result_doc;
    OpenSearchResult() {
        ref_count = 0;
        num_fields = 0;
        result_json = "";
        command_type = "";
    }
} OpenSearchResult;

#endif
#endif
