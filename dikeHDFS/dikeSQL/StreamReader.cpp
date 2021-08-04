#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>

#include "StreamReader.hpp"
#include "dikeSQLite3.h"

#ifndef SQLITE_OMIT_VIRTUALTABLE

/* Forward references to the various virtual table methods implemented
** in this file. */
static int srd_Create(sqlite3*, void*, int, const char*const*, sqlite3_vtab**,char**);
static int srd_Connect(sqlite3*, void*, int, const char*const*, sqlite3_vtab**,char**);
static int srd_BestIndex(sqlite3_vtab*,sqlite3_index_info*);
static int srd_Disconnect(sqlite3_vtab*);
static int srd_Open(sqlite3_vtab*, sqlite3_vtab_cursor**);
static int srd_Close(sqlite3_vtab_cursor*);
static int srd_Filter(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                        int argc, sqlite3_value **argv);
static int srd_Next(sqlite3_vtab_cursor*);
static int srd_Eof(sqlite3_vtab_cursor*);
static int srd_Column(sqlite3_vtab_cursor*,sqlite3_context*,int);
static int srd_Rowid(sqlite3_vtab_cursor*,sqlite3_int64*);

/* An instance of the virtual table */
typedef struct srdTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  DikeAsyncReader * reader;  
  //unsigned char cTypes[64];       /* Column affinity */
} srdTable;

/* A cursor for the virtual table */
typedef struct srdCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  DikeAsyncReader * reader;
  sqlite3_int64 iRowid;           /* The current rowid.  Negative for EOF */
} srdCursor;

/* This method is the destructor fo a srdTable object. */
static int srd_Disconnect(sqlite3_vtab *pVtab){
  srdTable *p = (srdTable*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

static int srd_Connect( sqlite3 *db, void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr)
{
    srdTable *pTable = 0;     /* The srdTable object to construct */  
    int rc = SQLITE_OK;     /* Result code from this routine */  
    std::string schema;
    DikeAsyncReader * reader = (DikeAsyncReader*)pAux;
    
    pTable =(srdTable *) sqlite3_malloc( sizeof(srdTable) );
    *ppVtab = (sqlite3_vtab*)pTable;
    if( pTable==0 ) {    
      std::cerr << "out of memory" << std::endl;
      return SQLITE_ERROR;
    }

    memset(pTable, 0, sizeof(srdTable));  

    schema = std::string("CREATE TABLE S3Object (" + reader->getSchema() + ")");
    //std::cout << "Schema: " << schema << std::endl;
      
    pTable->reader = reader;
  
    rc = sqlite3_declare_vtab(db, schema.c_str());
    if( rc ){
        std::cerr << "Bad schema: " <<  schema << " - " << sqlite3_errmsg(db) << std::endl;
        srd_Disconnect(&pTable->base);
        return SQLITE_ERROR;
    }

    sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
    return SQLITE_OK;
}

/*
** Reset the current row content held by a srdCursor.
*/
static void srd_CursorRowReset(srdCursor *pCur) {}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int srd_Create(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr)
{
    return srd_Connect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a srdCursor.
*/
static int srd_Close(sqlite3_vtab_cursor *cur){
  srdCursor *pCur = (srdCursor*)cur;
  srd_CursorRowReset(pCur);  
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new srdTable cursor object.
*/
static int srd_Open(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    srdTable *pTab = (srdTable*)p;      
    size_t nByte = sizeof(srdCursor);
    srdCursor * pCur = (srdCursor *)sqlite3_malloc64(nByte);
    if( pCur==0 ) {
        return SQLITE_NOMEM;
    }
    memset(pCur, 0, nByte);

    *ppCursor = &pCur->base;
    pCur->reader = pTab->reader;

    sqlite3_free(pTab->base.zErrMsg);
    pTab->base.zErrMsg = NULL;    
    return SQLITE_OK;
}

/*
** Advance a srdCursor to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int srd_Next(sqlite3_vtab_cursor *cur)
{
    srdCursor *pCur = (srdCursor*)cur;

    if(pCur->reader->readRecord()) {
        pCur->iRowid = -1;
    } else {
        pCur->iRowid++;
    }

    return SQLITE_OK;
}

/*
** Return values of columns for the row at which the srdCursor
** is currently pointing.
*/
static int srd_Column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col)
{
    srdCursor *pCur = (srdCursor*)cur;
    void * value;
    int len;
    sqlite_aff_t affinity;
 
    pCur->reader->getColumnValue(col, &value, &len, &affinity);
    switch(affinity){
        case SQLITE_AFF_TEXT_TERM:
            dike_sqlite3_result_text(ctx, (const char*)value, len, true, SQLITE_AFF_TEXT_TERM);
            break;
        case SQLITE_AFF_TEXT:
            //dike_sqlite3_result_text(ctx, (const char*)value, len, true, SQLITE_AFF_TEXT_TERM);
            sqlite3_result_text(ctx, (const char*)value, len, SQLITE_TRANSIENT);
            break;
        case SQLITE_AFF_INTEGER:
            sqlite3_result_int64(ctx, *(int64_t*)value);
            break;
        case SQLITE_AFF_NUMERIC:
            sqlite3_result_double(ctx, *(double*)value);
            break;
    }

    return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int srd_Rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
    srdCursor *pCur = (srdCursor*)cur;
    *pRowid = pCur->iRowid;
    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int srd_Eof(sqlite3_vtab_cursor *cur){
    srdCursor *pCur = (srdCursor*)cur;
    if(pCur->iRowid < 0) {
        return true;
    }
    
    return false;
}

static int srd_Filter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv)
{
    srdCursor *pCur = (srdCursor*)pVtabCursor;
    srdTable *pTab = (srdTable*)pVtabCursor->pVtab;
#if 0
    std::cout << "srd_Filter argc " << argc << std::endl;
    for(int i = 0; i < argc; i++){
        int sqliteType = sqlite3_value_type(argv[i]);
        std::cout << "sqliteType " << sqliteType << std::endl;
        if(sqliteType == SQLITE_NULL) {
            std::cout << "i : " << i << " SQLITE_NULL" << std::endl;
        }
        if(sqliteType == SQLITE_TEXT) {
            const unsigned char * text = sqlite3_value_text(argv[i]);
            int len = sqlite3_value_bytes(argv[i]);
            std::string s(text, text + len);
            std::cout << "srd_Filter " << s << std::endl;
        }
    }
#endif    
    pCur->iRowid = 0;
    return srd_Next(pVtabCursor);
}


void printConstraints(sqlite3_index_info *pIdxInfo)
{
    for(int i = 0; i < pIdxInfo->nConstraint; i++){
        std::cout << "Col " << pIdxInfo->aConstraint[i].iColumn;
        switch(pIdxInfo->aConstraint[i].op){
            case SQLITE_INDEX_CONSTRAINT_EQ:
            std::cout << " EQ ";
            break;
            case SQLITE_INDEX_CONSTRAINT_GT:
            std::cout << " GT ";
            break;
            case SQLITE_INDEX_CONSTRAINT_LE:
            std::cout << " LE ";
            break;
            case SQLITE_INDEX_CONSTRAINT_LT:
            std::cout << " LT ";
            break;
            case SQLITE_INDEX_CONSTRAINT_GE:
            std::cout << " GE ";
            break;
            case SQLITE_INDEX_CONSTRAINT_MATCH:
            std::cout << " MATCH ";
            break;
            case SQLITE_INDEX_CONSTRAINT_GLOB:
            std::cout << " GLOB ";
            break;
            case SQLITE_INDEX_CONSTRAINT_REGEXP:
            std::cout << " REGEXP ";
            break;
            case SQLITE_INDEX_CONSTRAINT_NE:
            std::cout << " NE ";
            break;
            case SQLITE_INDEX_CONSTRAINT_ISNOT:
            std::cout << " ISNOT ";
            break;
            case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
            std::cout << " ISNOTNULL ";
            break;
            case SQLITE_INDEX_CONSTRAINT_ISNULL:
            std::cout << " ISNULL ";
            break;
            case SQLITE_INDEX_CONSTRAINT_IS:
            std::cout << " IS ";
            break;
            case SQLITE_INDEX_CONSTRAINT_FUNCTION:
            std::cout << " FUNCTION ";
            break;
       }
       if (pIdxInfo->aConstraint[i].usable) {
           std::cout << " Usable "<< std::endl;
       } else {
           std::cout << " Not usable " << std::endl;
       }
    }
}

/*
** Only a forward full table scan is supported.  xBestIndex is mostly
** a no-op.  If CSVTEST_FIDX is set, then the presence of equality
** constraints lowers the estimated cost, which is fiction, but is useful
** for testing certain kinds of virtual table behavior.
*/
static int srd_BestIndex( sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    srdTable *pTab = (srdTable*)tab;  
    pTab->reader->setColUsed(pIdxInfo->colUsed);

#if 0
    printConstraints(pIdxInfo);

    int argvIndex = 1;
    for(int i = 0; i < pIdxInfo->nConstraint; i ++) {
        pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex++;
        pIdxInfo->aConstraintUsage[i].omit = 1; // Constraint is assumed to be fully handled by the virtual table
    }
    
    pIdxInfo->estimatedCost = 100;    
#endif

    return SQLITE_OK;
}

static sqlite3_module StreamReaderModule = {
    0,                     /* iVersion */
    srd_Create,            /* xCreate */
    srd_Connect,           /* xConnect */
    srd_BestIndex,         /* xBestIndex */
    srd_Disconnect,        /* xDisconnect */
    srd_Disconnect,        /* xDestroy */
    srd_Open,              /* xOpen - open a cursor */
    srd_Close,             /* xClose - close a cursor */
    srd_Filter,            /* xFilter - configure scan constraints */
    srd_Next,              /* xNext - advance a cursor */
    srd_Eof,               /* xEof - check for end of scan */
    srd_Column,            /* xColumn - read data */
    srd_Rowid,             /* xRowid - read data */
    0,                     /* xUpdate */
    0,                     /* xBegin */
    0,                     /* xSync */
    0,                     /* xCommit */
    0,                     /* xRollback */
    0,                     /* xFindMethod */
    0,                     /* xRename */
};

#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */

/* 
** This routine is called when the extension is loaded.  The new
** SRD virtual table module is registered with the calling database
** connection.
*/
int sqlite3_srd_init( sqlite3 *db, char **errMsg, const sqlite3_api_routines *pApi)
{
  SQLITE_EXTENSION_INIT2(pApi);
  return sqlite3_create_module(db, "StreamReader", &StreamReaderModule, 0);
}

int StreamReaderInit(sqlite3 *db, DikeAsyncReader * dikeAsyncReader)
{
  SQLITE_EXTENSION_INIT2(NULL);
  return sqlite3_create_module(db, "StreamReader", &StreamReaderModule, dikeAsyncReader);
}
