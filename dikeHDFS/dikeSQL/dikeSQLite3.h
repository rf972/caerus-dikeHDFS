#ifndef DIKE_SQLITE_3_H
#define DIKE_SQLITE_3_H
#include <sqlite3ext.h>

extern "C" {
int dike_sqlite3_result_text( sqlite3_context *pCtx, const char *z, int n, int affinity);
int dike_sqlite3_get_data(sqlite3_stmt *pStmt, const char ** res, int res_size, int * total_bytes);
}
#endif /*DIKE_SQLITE_3_H */