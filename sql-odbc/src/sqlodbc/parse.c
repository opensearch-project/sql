/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


#include <ctype.h>
#include <stdio.h>
#include <string.h>

#include "catfunc.h"
#include "opensearch_odbc.h"
#include "opensearch_types.h"
#include "misc.h"
#include "multibyte.h"
#include "opensearch_apifunc.h"
#include "opensearch_connection.h"
#include "opensearch_info.h"
#include "qresult.h"
#include "statement.h"

Int4 FI_precision(const FIELD_INFO *fi) {
    OID ftype;

    if (!fi)
        return -1;
    ftype = FI_type(fi);
    switch (ftype) {
        case OPENSEARCH_TYPE_NUMERIC:
            return fi->column_size;
        case OPENSEARCH_TYPE_DATETIME:
        case OPENSEARCH_TYPE_TIMESTAMP_NO_TMZONE:
            return fi->decimal_digits;
    }
    return 0;
}

static void setNumFields(IRDFields *irdflds, size_t numFields) {
    FIELD_INFO **fi = irdflds->fi;
    size_t nfields = irdflds->nfields;

    if (numFields < nfields) {
        int i;

        for (i = (int)numFields; i < (int)nfields; i++) {
            if (fi[i])
                fi[i]->flag = 0;
        }
    }
    irdflds->nfields = (UInt4)numFields;
}

void SC_initialize_cols_info(StatementClass *stmt, BOOL DCdestroy,
                             BOOL parseReset) {
    IRDFields *irdflds = SC_get_IRDF(stmt);

    /* Free the parsed table information */
    if (stmt->ti) {
        TI_Destructor(stmt->ti, stmt->ntab);
        free(stmt->ti);
        stmt->ti = NULL;
    }
    stmt->ntab = 0;
    if (DCdestroy) /* Free the parsed field information */
        DC_Destructor((DescriptorClass *)SC_get_IRD(stmt));
    else
        setNumFields(irdflds, 0);
    if (parseReset) {
        stmt->parse_status = STMT_PARSE_NONE;
        SC_reset_updatable(stmt);
    }
}
