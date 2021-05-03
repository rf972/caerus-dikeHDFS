#include "sqlite3.c"

int dike_sqlite3_result_text(
  sqlite3_context *pCtx,
  const char *z,
  int n,
  int isCopyRequiered,
  int affinity)
{    
    //setResultStrOrError(pCtx, z, n, SQLITE_UTF8, xDel);
    //sqlite3VdbeMemSetStr(pCtx->pOut, z, n, enc, xDel)
    Mem *pMem = pCtx->pOut;
    int nByte = n;      /* New value for pMem->n */
    int iLimit = SQLITE_MAX_LENGTH;         /* Maximum allowed string or blob size */
    u16 flags = 0;      /* New value for pMem->flags */
    int szNew;

    /* If z is a NULL pointer, set pMem to contain an SQL NULL. */
    if( !z ){
        sqlite3VdbeMemSetNull(pMem);
        return SQLITE_OK;
    }

    flags = MEM_Str | MEM_Term;

    if(isCopyRequiered) {
        szNew = (int)MAX(nByte,32);
        if( pMem->szMalloc < szNew ){
            if( sqlite3VdbeMemGrow(pMem, szNew, 0) ){
                return SQLITE_NOMEM_BKPT;
            }
        } else {
            pMem->z = pMem->zMalloc;
        }
        memcpy(pMem->z, z, nByte);
    } else {
        sqlite3VdbeMemRelease(pMem);
        pMem->z = (char *)z;
        pMem->xDel = SQLITE_STATIC;
        flags |= MEM_Static;
    }    
#if 0 /* This looks to be expencive */    
    if(affinity == SQLITE_AFF_INTEGER){
        pMem->u.i = sqlite3Atoi(pMem->z);
        flags |= MEM_Int;
    } else if(affinity == SQLITE_AFF_NUMERIC){
        sqlite3AtoF(pMem->z, &pMem->u.r, nByte - 1, SQLITE_UTF8);
        flags |= MEM_Real; // MEM_IntReal
    }
#endif

    pMem->n = nByte - 1;
    pMem->flags = flags;
    pMem->enc = SQLITE_UTF8;

    return SQLITE_OK;
}

int dike_sqlite3_get_data(sqlite3_stmt *pStmt, const char ** res, int res_size, int * total_bytes)
{
  Vdbe *pVm = (Vdbe *)pStmt;
  int i;
  u16 flags = (MEM_Str|MEM_Term);
  * total_bytes = 0;

  if( pVm==0 || pVm->pResultSet==0 || res_size < pVm->nResColumn) return 0;

  for(i =0; i < pVm->nResColumn; i++){
    if(pVm->pResultSet[i].flags & flags == flags){
        res[i] = pVm->pResultSet[i].z; 
    } else {
        res[i] = sqlite3ValueText(&pVm->pResultSet[i], SQLITE_UTF8);
    }
    *total_bytes += pVm->pResultSet[i].n;
  }
  return pVm->nResColumn;
}



