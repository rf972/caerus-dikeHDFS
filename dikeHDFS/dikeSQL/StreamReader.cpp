#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>
#include <unistd.h>

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
  int nCol;                       /* Number of columns */
  unsigned char cTypes[64];       /* Column affinity */
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

static int srd_parse_until(const char * str, const char *expr, int * pos)
{
  int p = *pos;  
  int expr_len = strlen(expr);
  int i;

  while(str[p] != 0) {
    for(i = 0; i < expr_len; i++){
      if(str[p]==expr[i]){
        *pos = p + 1; /* We returning "interesting" position */
        return SQLITE_OK;  
      }
    }
    p++;
  }
  
  return SQLITE_ERROR;
}

static int srd_parse_skip(const char * str, int c, int * pos)
{
  int p = *pos;

  while(str[p] != 0) {
    if(str[p]!=c) {
      *pos = p;
      return SQLITE_OK;
    }
    p++;
  }

  return SQLITE_ERROR;
}

static int srd_parse_schema(const char * schema, srdTable *pTable)
{
  int pos = 0;
  int len = strlen(schema);
  int col = 0; /* column counter */
  
  /* parse until opening bracket */
  if(srd_parse_until(schema, "(", &pos)){
    return SQLITE_ERROR;
  }

  while(schema[pos-1] != ')') {
    if(srd_parse_skip(schema, ' ', &pos)){
      return SQLITE_ERROR;
    }

    if(schema[pos]=='"'){ /* If quote , parse until end of it */
      pos++;
      if(srd_parse_until(schema, "\"", &pos)){
        return SQLITE_ERROR;
      }
     } else { /* parse until space */
      if(srd_parse_skip(schema, ' ', &pos)){
        return SQLITE_ERROR;
      }
    }

    if(srd_parse_until(schema, "), ", &pos)){
      return SQLITE_ERROR;
    }
    
    /* if we found ',' or '\n' - we do not have data type */
    if(schema[pos-1]==',' || schema[pos-1]==')') {
      pTable->cTypes[col] = SQLITE_AFF_TEXT;
      col++;
      continue;
    }

    if(srd_parse_skip(schema, ' ', &pos)){
      return SQLITE_ERROR;
    }

    /* Main logic */
    switch(schema[pos]) {
      case 'N':
        if(schema[pos+1]=='U'){
          pTable->cTypes[col] = SQLITE_AFF_NUMERIC;
        } else{
          pTable->cTypes[col] = SQLITE_AFF_NONE;
        }
        break;
      case 'B':
        pTable->cTypes[col] = SQLITE_AFF_BLOB;
        break;
      case 'T':
        pTable->cTypes[col] = SQLITE_AFF_TEXT;
        break;
      case 'I':
        pTable->cTypes[col] = SQLITE_AFF_INTEGER;
        break;
      case 'R':
        pTable->cTypes[col] = SQLITE_AFF_REAL;
        break;
      default:
        pTable->cTypes[col] = SQLITE_AFF_TEXT;
        break;                        
    }

    if(srd_parse_until(schema, ",)" , &pos)){
      return SQLITE_ERROR;
    }

    col++;
  } 

  pTable->nCol = col;  

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
    StreamReaderParam * param = (StreamReaderParam*)pAux;
    
    pTable =(srdTable *) sqlite3_malloc( sizeof(srdTable) );
    *ppVtab = (sqlite3_vtab*)pTable;
    if( pTable==0 ) {    
      std::cerr << "out of memory" << std::endl;
      return SQLITE_ERROR;
    }

    memset(pTable, 0, sizeof(srdTable));  
  
    if(param->reader->blockOffset > 0){
      param->reader->seekRecord();
    } else if (param->headerInfo != HEADER_INFO_NONE) {
      param->reader->seekRecord();
    }
  
    pTable->nCol = param->reader->getColumnCount();

    schema = std::string("CREATE TABLE S3Object (");
    for(int i = 0; i < pTable->nCol; i++) {
      schema += "_" +  std::to_string(i+1);
      if (i < pTable->nCol -1 ){
        schema += ",";
      } else {
        schema += ")";
      }
    }

    //std::cout << "Schema: " << schema << std::endl;
  
    param->reader->initRecord(pTable->nCol);
    pTable->reader = param->reader;
  
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
    srdCursor *pCur;
    size_t nByte;
    nByte = sizeof(*pCur) + (sizeof(char*)+sizeof(int)+sizeof(char*))*pTab->nCol;
    pCur = (srdCursor *)sqlite3_malloc64( nByte );
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
  srdTable *pTab = (srdTable*)cur->pVtab;
 
  if( col >= 0 && col < pTab->nCol){
    dike_sqlite3_result_text(ctx, 
                            (const char*)pCur->reader->record->fields[col], 
                            pCur->reader->record->len[col], 
                            true, // Copy requiered
                            pTab->cTypes[col]);

    //dike_sqlite3_result_text(ctx, (const char*)pCur->reader->record->fields[i], pCur->reader->record->len[i], pTab->cTypes[i]);
    //sqlite3_result_text(ctx, (const char*)pCur->reader->record->fields[i], -1 , SQLITE_TRANSIENT);
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

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int srd_Filter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv)
{
    srdCursor *pCur = (srdCursor*)pVtabCursor;
    srdTable *pTab = (srdTable*)pVtabCursor->pVtab;
    pCur->iRowid = 0;
    return srd_Next(pVtabCursor);
}

/*
** Only a forward full table scan is supported.  xBestIndex is mostly
** a no-op.  If CSVTEST_FIDX is set, then the presence of equality
** constraints lowers the estimated cost, which is fiction, but is useful
** for testing certain kinds of virtual table behavior.
*/
static int srd_BestIndex( sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    pIdxInfo->estimatedCost = 1000000;
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

int StreamReaderInit(sqlite3 *db, StreamReaderParam * param)
{
  SQLITE_EXTENSION_INIT2(NULL);
  return sqlite3_create_module(db, "StreamReader", &StreamReaderModule, param);
}
