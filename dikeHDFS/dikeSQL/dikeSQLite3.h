#ifndef DIKE_SQLITE_3_H
#define DIKE_SQLITE_3_H
#include <sqlite3ext.h>

extern "C" {
int dike_sqlite3_result_text( sqlite3_context *pCtx, const char *z, int n, void (*xDel)(void *));

}
#endif /*DIKE_SQLITE_3_H */