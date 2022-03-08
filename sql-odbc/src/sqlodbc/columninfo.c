#include "columninfo.h"

#include <stdlib.h>
#include <string.h>

#include "opensearch_types.h"
#include "opensearch_apifunc.h"
#include "opensearch_connection.h"

ColumnInfoClass *CI_Constructor(void) {
    ColumnInfoClass *rv;

    rv = (ColumnInfoClass *)malloc(sizeof(ColumnInfoClass));

    if (rv) {
        rv->refcount = 0;
        rv->num_fields = 0;
        rv->coli_array = NULL;
    }

    return rv;
}

void CI_Destructor(ColumnInfoClass *self) {
    CI_free_memory(self);

    free(self);
}

void CI_free_memory(ColumnInfoClass *self) {
    register Int2 lf;
    int num_fields = self->num_fields;

    /* Safe to call even if null */
    self->num_fields = 0;
    if (self->coli_array) {
        for (lf = 0; lf < num_fields; lf++) {
            if (self->coli_array[lf].name) {
                free(self->coli_array[lf].name);
                self->coli_array[lf].name = NULL;
            }
        }
        free(self->coli_array);
        self->coli_array = NULL;
    }
}

void CI_set_num_fields(ColumnInfoClass *self, SQLSMALLINT new_num_fields) {
    CI_free_memory(self); /* always safe to call */

    self->num_fields = new_num_fields;

    self->coli_array =
        (struct srvr_info *)calloc(sizeof(struct srvr_info), self->num_fields);
}

void CI_set_field_info(ColumnInfoClass *self, int field_num,
                       const char *new_name, OID new_adtid, Int2 new_adtsize,
                       Int4 new_atttypmod, OID new_relid, OID new_attid) {
    /* check bounds */
    if ((field_num < 0) || (field_num >= self->num_fields))
        return;

    /* store the info */
    self->coli_array[field_num].name = strdup(new_name);
    self->coli_array[field_num].adtid = new_adtid;
    self->coli_array[field_num].adtsize = new_adtsize;
    self->coli_array[field_num].atttypmod = new_atttypmod;

    self->coli_array[field_num].display_size = OPENSEARCH_ADT_UNSET;
    self->coli_array[field_num].relid = new_relid;
    self->coli_array[field_num].attid = (short)new_attid;
}
