/*	TryEnterCriticalSection needs the following #define */
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0400
#endif /* _WIN32_WINNT */

#include "opensearch_connection.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>

#include "misc.h"

/* for htonl */
#ifdef WIN32
#include <Winsock2.h>
#else
#include <arpa/inet.h>
#endif

#include <map>
#include <string>

#include "dlg_specific.h"
#include "environ.h"
#include "loadlib.h"
#include "multibyte.h"
#include "opensearch_apifunc.h"
#include "opensearch_helper.h"
#include "qresult.h"
#include "statement.h"

#define PROTOCOL3_OPTS_MAX 30
#define ERROR_BUFF_SIZE 200
#define OPTION_COUNT 4
#if OPTION_COUNT > PROTOCOL3_OPTS_MAX
#error("Option count (OPTION_COUNT) is greater than max option count allow (PROTOCOL3_OPTS_MAX).")
#endif

void CC_determine_locale_encoding(ConnectionClass *self);

char CC_connect(ConnectionClass *self) {
    if (self == NULL)
        return 0;

    // Attempt to connect to OpenSearch
    int conn_code = LIBOPENSEARCH_connect(self);
    if (conn_code <= 0)
        return static_cast< char >(conn_code);

    // Set encodings
    CC_determine_locale_encoding(self);
#ifdef UNICODE_SUPPORT
    if (CC_is_in_unicode_driver(self)) {
        if (!SQL_SUCCEEDED(CC_send_client_encoding(self, "UTF8"))) {
            return 0;
        }
    } else
#endif
    {
        if (!SQL_SUCCEEDED(
                CC_send_client_encoding(self, self->locale_encoding))) {
            return 0;
        }
    }

    // Set cursor parameters based on connection info
    self->status = CONN_CONNECTED;
    if ((CC_is_in_unicode_driver(self)) && (CC_is_in_ansi_app(self)))
        self->unicode |= CONN_DISALLOW_WCHAR;

    // 1 is SQL_SUCCESS and 2 is SQL_SCCUESS_WITH_INFO
    return 1;
}

int LIBOPENSEARCH_connect(ConnectionClass *self) {
    if (self == NULL)
        return 0;

    // Setup options
    runtime_options rt_opts;

    // Connection
    rt_opts.conn.server.assign(self->connInfo.server);
    rt_opts.conn.port.assign(self->connInfo.port);
    rt_opts.conn.timeout.assign(self->connInfo.response_timeout);

    // Authentication
    rt_opts.auth.auth_type.assign(self->connInfo.authtype);
    rt_opts.auth.username.assign(self->connInfo.username);
    rt_opts.auth.password.assign(SAFE_NAME(self->connInfo.password));
    rt_opts.auth.region.assign(self->connInfo.region);

    // Encryption
    rt_opts.crypt.verify_server = (self->connInfo.verify_server == 1);
    rt_opts.crypt.use_ssl = (self->connInfo.use_ssl == 1);

    void *opensearchconn = OpenSearchConnectDBParams(rt_opts, FALSE, OPTION_COUNT);
    if (opensearchconn == NULL) {
        std::string err = GetErrorMsg(opensearchconn);
        CC_set_error(self, CONN_OPENDB_ERROR,
                     (err.empty()) ? "OpenSearchConnectDBParams error" : err.c_str(),
                     "LIBOPENSEARCH_connect");
        return 0;
    }

    // Check connection status
    if (OpenSearchStatus(opensearchconn) != CONNECTION_OK) {
        std::string msg = GetErrorMsg(opensearchconn);
        char error_message_out[ERROR_BUFF_SIZE] = "";
        if (!msg.empty())
            SPRINTF_FIXED(error_message_out, "Connection error: %s",
                          msg.c_str());
        else
            STRCPY_FIXED(error_message_out,
                         "Connection error: No message available.");
        CC_set_error(self, CONN_OPENDB_ERROR, error_message_out,
                     "LIBOPENSEARCH_connect");
        OpenSearchDisconnect(opensearchconn);
        return 0;
    }

    // Set server version
    std::string server_version = GetServerVersion(opensearchconn);
    STRCPY_FIXED(self->opensearch_version, server_version.c_str());

    std::string cluster_name = GetClusterName(opensearchconn);
    STRCPY_FIXED(self->cluster_name, cluster_name.c_str());

    self->opensearchconn = (void *)opensearchconn;
    return 1;
}

// TODO #36 - When we fix encoding, we should look into returning a code here.
// This is called in connection.c and the return code isn't checked
void CC_set_locale_encoding(ConnectionClass *self, const char *encoding) {
    if (self == NULL)
        return;

    // Set encoding
    char *prev_encoding = self->locale_encoding;
    self->locale_encoding = (encoding == NULL) ? NULL : strdup(encoding);
    if (prev_encoding)
        free(prev_encoding);
}

// TODO #36 - Add return code - see above function comment
void CC_determine_locale_encoding(ConnectionClass *self) {
    // Don't update if it's already set
    if ((self == NULL) || (self->locale_encoding != NULL))
        return;

    // Get current db encoding and derive the locale encoding
    // TODO #34 - Investigate locale
    CC_set_locale_encoding(self, "SQL_ASCII");
}

int CC_send_client_encoding(ConnectionClass *self, const char *encoding) {
    if ((self == NULL) || (encoding == NULL))
        return SQL_ERROR;

    // Update client encoding
    std::string des_db_encoding(encoding);
    std::string cur_db_encoding = OpenSearchGetClientEncoding(self->opensearchconn);
    if (des_db_encoding != cur_db_encoding) {
        if (!OpenSearchSetClientEncoding(self->opensearchconn, des_db_encoding)) {
            return SQL_ERROR;
        }
    }

    // Update connection class to reflect updated client encoding
    char *prev_encoding = self->original_client_encoding;
    self->original_client_encoding = strdup(des_db_encoding.c_str());
    self->ccsc = static_cast< short >(opensearch_CS_code(des_db_encoding.c_str()));
    self->mb_maxbyte_per_char = static_cast< short >(opensearch_mb_maxlen(self->ccsc));
    if (prev_encoding != NULL)
        free(prev_encoding);

    return SQL_SUCCESS;
}

void CC_initialize_opensearch_version(ConnectionClass *self) {
    STRCPY_FIXED(self->opensearch_version, "7.4");
    self->opensearch_version_major = 7;
    self->opensearch_version_minor = 4;
}

void LIBOPENSEARCH_disconnect(void *conn) {
    OpenSearchDisconnect(conn);
}
