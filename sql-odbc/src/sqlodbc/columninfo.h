#ifndef __COLUMNINFO_H__
#define __COLUMNINFO_H__

#include "opensearch_odbc.h"

struct ColumnInfoClass_ {
    UInt4 refcount; /* reference count. A ColumnInfo can be shared by
                     * several qresults. */
    Int2 num_fields;
    struct srvr_info {
        char *name;        /* field name */
        OID adtid;         /* type oid */
        Int2 adtsize;      /* type size */
        Int4 display_size; /* the display size (longest row) */
        Int4 atttypmod;    /* the length of bpchar/varchar */
        OID relid;         /* the relation id */
        Int2 attid;        /* the attribute number */
    } * coli_array;
};

#define CI_get_num_fields(self) (self->num_fields)
#define CI_get_oid(self, col) (self->coli_array[col].adtid)
#define CI_get_fieldname(self, col) (self->coli_array[col].name)
#define CI_get_fieldsize(self, col) (self->coli_array[col].adtsize)
#define CI_get_display_size(self, col) (self->coli_array[col].display_size)
#define CI_get_atttypmod(self, col) (self->coli_array[col].atttypmod)
#define CI_get_relid(self, col) (self->coli_array[col].relid)
#define CI_get_attid(self, col) (self->coli_array[col].attid)

ColumnInfoClass *CI_Constructor(void);
void CI_Destructor(ColumnInfoClass *self);
void CI_free_memory(ColumnInfoClass *self);

/* functions for setting up the fields from within the program, */
/* without reading from a socket */
void CI_set_num_fields(ColumnInfoClass *self, SQLSMALLINT new_num_fields);

// Used in opensearch_parse_results.cpp
#ifdef __cplusplus
extern "C" {
#endif
void CI_set_field_info(ColumnInfoClass *self, int field_num,
                       const char *new_name, OID new_adtid, Int2 new_adtsize,
                       Int4 atttypmod, OID new_relid, OID new_attid);
#ifdef __cplusplus
}
#endif

#endif
