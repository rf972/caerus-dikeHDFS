#include "sqlite3.c"

int dike_sqlite3_result_text(
  sqlite3_context *pCtx,
  const char *z,
  int n,
  void (*xDel)(void *) )
{
    
    //setResultStrOrError(pCtx, z, n, SQLITE_UTF8, xDel);
    //sqlite3VdbeMemSetStr(pCtx->pOut, z, n, enc, xDel)
    Mem *pMem = pCtx->pOut;
    int nByte = n;      /* New value for pMem->n */
    int iLimit = SQLITE_MAX_LENGTH;         /* Maximum allowed string or blob size */
    u16 flags = 0;      /* New value for pMem->flags */

    /* If z is a NULL pointer, set pMem to contain an SQL NULL. */
    if( !z ){
        sqlite3VdbeMemSetNull(pMem);
        return SQLITE_OK;
    }

    flags = MEM_Str | MEM_Term;

    /* The following block sets the new values of Mem.z and Mem.xDel. It
    ** also sets a flag in local variable "flags" to indicate the memory
    ** management (one of MEM_Dyn or MEM_Static).
    */
    if( xDel==SQLITE_TRANSIENT ){
        u32 nAlloc = nByte;
        if( sqlite3VdbeMemClearAndResize(pMem, (int)MAX(nAlloc,32)) ){
            return SQLITE_NOMEM_BKPT;
        }
        memcpy(pMem->z, z, nAlloc);
    }

    pMem->n = nByte;
    pMem->flags = flags;
    pMem->enc = SQLITE_UTF8;

    return SQLITE_OK;
}

