#ifndef DIKE_SQLITE_3_H
#define DIKE_SQLITE_3_H
#include <sqlite3ext.h>
#include <stdint.h>
#include "DikeBinaryColumn.h"

extern "C" {
int dike_sqlite3_result_text( sqlite3_context *pCtx, const char *z, int n, int isCopyRequiered, int affinity);
int dike_sqlite3_get_data(sqlite3_stmt *pStmt, const char ** res, int res_size, int * total_bytes);
double dike_sqlite3_get_double(sqlite3_stmt *pStmt, int i);
int64_t dike_sqlite3_get_int64(sqlite3_stmt *pStmt, int i);
int dike_sqlite3_get_bytes(sqlite3_stmt *pStmt, int i, uint8_t ** bytes);
int dike_sqlite3_get_results(sqlite3_stmt *pStmt, DikeBinaryColumn_t ** columns, int * flush_needed);
}
#endif /*DIKE_SQLITE_3_H */