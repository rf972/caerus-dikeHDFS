
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h> 
#include <unistd.h>

#include "StreamReader.hpp"


#ifndef SQLITE_OMIT_VIRTUALTABLE

/* This is a copy for sqlite3.c file */
#define SQLITE_AFF_NONE     0x40  /* '@' */
#define SQLITE_AFF_BLOB     0x41  /* 'A' */
#define SQLITE_AFF_TEXT     0x42  /* 'B' */
#define SQLITE_AFF_NUMERIC  0x43  /* 'C' */
#define SQLITE_AFF_INTEGER  0x44  /* 'D' */
#define SQLITE_AFF_REAL     0x45  /* 'E' */

/*
** A macro to hint to the compiler that a function should not be
** inlined.
*/
#if defined(__GNUC__)
#  define CSV_NOINLINE  __attribute__((noinline))
#  define CSV_ALWAYS_INLINE  __attribute__((always_inline)) inline
#  define CSV_LIKELY(x)      __builtin_expect(!!(x), 1)
#  define CSV_UNLIKELY(x)    __builtin_expect(!!(x), 0)
#elif defined(_MSC_VER) && _MSC_VER>=1310
#  define CSV_NOINLINE  __declspec(noinline)
#else
#  define CSV_NOINLINE
#endif


/* Max size of the error message in a StreamReader */
#define SRD_MXERR 200

/* Size of the StreamReader input buffer */
#define SRD_INBUFSZ (1<<20)

static char FDELIM = '|';
static char RDELIM = '\n';

/* A context object used when read a CSV file. */
typedef struct StreamReader StreamReader;
struct StreamReader {
  //FILE *in;              /* Read the CSV text from this input stream */
  std::istream * in;
  char *z;               /* Accumulated text for a field */
  int n;                 /* Number of bytes in z */
  int nAlloc;            /* Space allocated for z[] */
  int nLine;             /* Current line number */
  //int bNotFirst;       /* True if prior text has been seen */
  int cTerm;             /* Character that terminated the most recent field */
  size_t iIn;            /* Next unread character in the input buffer */
  size_t nIn;            /* Number of characters in the input buffer */
  char *zIn;             /* The input buffer */
  char *zStreamBuf[2];   /* Two temporary buffers for async input stream */
  char * zStream;        /* Input thread will read file here */
  int  nStream;          /* Number of characters in the nStream buffer */
  pthread_t thread;      /* Input stream thread id */
  sem_t sem;             /* Input stream syncronization semaphore */
  pthread_mutex_t lock; 

  char zErr[SRD_MXERR];  /* Error message */
};

/* Report an error on a StreamReader */
static void csv_errmsg(StreamReader *p, const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(SRD_MXERR, p->zErr, zFormat, ap);
  va_end(ap);
}

/* Initialize a StreamReader object */
static void srd_reader_init(StreamReader *p){ 
  p->in = 0;
  p->z = 0;
  p->n = 0;
  p->nAlloc = 0;
  p->nLine = 0;
  //p->bNotFirst = 0;
  p->nIn = 0;
  p->zIn = 0;
  p->zErr[0] = 0;
  p->zStreamBuf[0] = 0;
  p->zStreamBuf[1] = 0;
  p->nStream = -1;
}

/* Close and reset a StreamReader object */
static void csv_reader_reset(StreamReader *p){
  if( p->in ) {
    pthread_join(p->thread, NULL);
    free(p->zStreamBuf[0]);
    free(p->zStreamBuf[1]);
  }
  sqlite3_free(p->z);
  //srd_reader_init(p);
}

static std::size_t readFromStream( std::istream& in, char* buf, std::size_t len ) {
	std::size_t n = 0;
	
	while( len > 0 && in.good() ) {
		in.read( &buf[n], len );
		int i = in.gcount();
		n += i;
		len -= i;
	}
	
	return n;
}

static void *srd_stream_thread(void *arg)
{
  StreamReader *p = (StreamReader *)arg;
  
  while(1) {
    sem_wait(&p->sem);
    pthread_mutex_lock(&p->lock);
    
    //p->nStream = fread(p->zStream, 1, SRD_INBUFSZ, p->in);
    p->nStream = readFromStream(*p->in, p->zStream, SRD_INBUFSZ);
    if(p->nStream < SRD_INBUFSZ){
      p->zStream[p->nStream] = EOF;
    }
    if( p->nStream == 0 ){
      pthread_mutex_unlock(&p->lock);
      //p->zStream[0] = EOF;
      return 0;
    }
    pthread_mutex_unlock(&p->lock);
  }

  return 0;
}

/* Open the file associated with a StreamReader
** Return the number of errors.
*/
static int srd_reader_open(StreamReader *p, std::istream * in)
{ 
  p->in = in; 
  p->zStreamBuf[0] = (char *)malloc(SRD_INBUFSZ);
  p->zStreamBuf[1] = (char *)malloc(SRD_INBUFSZ);
  if(p->zStreamBuf[0]==0 || p->zStreamBuf[1]==0){
    csv_errmsg(p, "out of memory");
    return 1;
  }

  p->zStream = p->zStreamBuf[0];
  p->nStream = -1;
  sem_init(&p->sem,0,1);
  pthread_mutex_init(&p->lock, NULL);
  pthread_create(&p->thread,NULL,srd_stream_thread,p);
  
  return 0;
}

/* The input buffer has overflowed.  Refill the input buffer, then
** return the next character
*/
static CSV_NOINLINE int csv_getc_refill(StreamReader *p){
  size_t got;

  while(1){
    pthread_mutex_lock(&p->lock);    
    if(p->nStream == -1){ /* Unlikely timing race */
      pthread_mutex_unlock(&p->lock);
      usleep(100);
    } else{
      break;
    }
  }

  got = p->nStream;

  if( got==0 ) {
    pthread_mutex_unlock(&p->lock);
    return EOF;
  }

  p->zIn = p->zStream;
  if(p->zStream == p->zStreamBuf[0]){
    p->zStream = p->zStreamBuf[1];
  } else {
    p->zStream = p->zStreamBuf[0];
  }
  p->nStream = -1;
  pthread_mutex_unlock(&p->lock);

  sem_post(&p->sem);

  p->nIn = got;
  p->iIn = 1;
  return p->zIn[0];  
}

/* Return the next character of input.  Return EOF at end of input. */
static CSV_ALWAYS_INLINE int csv_getc(StreamReader *p){
  if(CSV_UNLIKELY( p->iIn >= p->nIn )){
    if( p->in!=0 ) return csv_getc_refill(p);
    return EOF;
  }
  return ((unsigned char*)p->zIn)[p->iIn++];
}

/* Increase the size of p->z and append character c to the end. 
** Return 0 on success and non-zero if there is an OOM error */
static CSV_NOINLINE int csv_resize_and_append(StreamReader *p, char c){
  char *zNew;
  int nNew = p->nAlloc*2 + 100;
  zNew = (char *)sqlite3_realloc64(p->z, nNew);
  if( CSV_LIKELY(zNew) ){
    p->z = zNew;
    p->nAlloc = nNew;
    p->z[p->n++] = c;
    return 0;
  }else{
    csv_errmsg(p, "out of memory");
    return 1;
  }
}

/* Append a single character to the StreamReader.z[] array.
** Return 0 on success and non-zero if there is an OOM error */
static CSV_ALWAYS_INLINE int csv_append(StreamReader *p, char c){
  if( CSV_UNLIKELY(p->n>=p->nAlloc-1)) return csv_resize_and_append(p, c);
  p->z[p->n++] = c;
  return 0;
}

/* Read a single field of CSV text.  Compatible with rfc4180 and extended
** with the option of having a separator other than ",".
**
**   +  Input comes from p->in.
**   +  Store results in p->z of length p->n.  Space to hold p->z comes
**      from sqlite3_malloc64().
**   +  Keep track of the line number in p->nLine.
**   +  Store the character that terminates the field in p->cTerm.  Store
**      EOF on end-of-file.
**
** Return 0 at EOF or on OOM.  On EOF, the p->cTerm character will have
** been set to EOF.
*/
static char *tbl_read_one_field(StreamReader *p, int delim){
  int c;
  p->n = 0;
  c = csv_getc(p);
  if( CSV_UNLIKELY(c==EOF) ){
    p->cTerm = EOF;
    return 0;
  }
  if( CSV_UNLIKELY(c=='"') ){
    int pc, ppc;
    int startLine = p->nLine;
    pc = ppc = 0;
    while( 1 ){
      c = csv_getc(p);
      if( c<='"' || pc=='"' ){
        if( c=='\n' ) p->nLine++;
        if( c=='"' ){
          if( pc=='"' ){
            pc = 0;
            continue;
          }
        }
        if( (c==delim && pc=='"')
         || (c=='\n' && pc=='"')
         || (c=='\n' && pc=='\r' && ppc=='"')
         || (c==EOF && pc=='"')
        ){
          do{ p->n--; }while( p->z[p->n]!='"' );
          p->cTerm = (char)c;
          break;
        }
        if( pc=='"' && c!='\r' ){
          csv_errmsg(p, "line %d: unescaped %c character", p->nLine, '"');
          break;
        }
        if( c==EOF ){
          csv_errmsg(p, "line %d: unterminated %c-quoted field\n",
                     startLine, '"');
          p->cTerm = (char)c;
          break;
        }
      }
      if( csv_append(p, (char)c) ) return 0;
      ppc = pc;
      pc = c;
    }
  } else {
    while(c!=0 && c!=delim && c!='\n' && c!=EOF){
      if( csv_append(p, (char)c) ) return 0;
      c = csv_getc(p);
    }
    if( c=='\n' ){
      p->nLine++;
      if( p->n>0 && p->z[p->n-1]=='\r' ) p->n--;
    }
    p->cTerm = (char)c;
  }
  if( p->z ) p->z[p->n] = 0;
  //p->bNotFirst = 1;
  return p->z;
}


/* Forward references to the various virtual table methods implemented
** in this file. */
static int srd_Create(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int srd_Connect(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
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
  
  std::istream * in;
  
  long iStart;                    /* Offset to start of data in zFilename */
  int nCol;                       /* Number of columns in the CSV file */
  unsigned int tstFlags;          /* Bit values used for testing */
  unsigned char cTypes[64];       /* Column affinity */
} srdTable;

/* A cursor for the virtual table */
typedef struct srdCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  StreamReader rdr;               /* The StreamReader object */
  char **azVal;                   /* Value of the current row */
  int *aLen;                      /* Length of each entry */
  char **azPtr;                   /* Deliminator indices to reader array */
  sqlite3_int64 iRowid;           /* The current rowid.  Negative for EOF */
} srdCursor;

/* Transfer error message text from a reader into a srdTable */
static void csv_xfer_error(srdTable *pTab, StreamReader *pRdr){
  sqlite3_free(pTab->base.zErrMsg);
  pTab->base.zErrMsg = sqlite3_mprintf("%s", pRdr->zErr);
}

/* This method is the destructor fo a srdTable object. */
static int srd_Disconnect(sqlite3_vtab *pVtab){
  srdTable *p = (srdTable*)pVtab;
  //sqlite3_free(p->zFilename);
  //sqlite3_free(p->zData);
  sqlite3_free(p);
  return SQLITE_OK;
}

/* Skip leading whitespace.  Return a pointer to the first non-whitespace
** character, or to the zero terminator if the string has only whitespace */
static const char *csv_skip_whitespace(const char *z){
  while( isspace((unsigned char)z[0]) ) z++;
  return z;
}

/* Remove trailing whitespace from the end of string z[] */
static void csv_trim_whitespace(char *z){
  size_t n = strlen(z);
  while( n>0 && isspace((unsigned char)z[n]) ) n--;
  z[n] = 0;
}

/* Dequote the string */
static void csv_dequote(char *z){
  int j;
  char cQuote = z[0];
  size_t i, n;

  if( cQuote!='\'' && cQuote!='"' ) return;
  n = strlen(z);
  if( n<2 || z[n-1]!=z[0] ) return;
  for(i=1, j=0; i<n-1; i++){
    if( z[i]==cQuote && z[i+1]==cQuote ) i++;
    z[j++] = z[i];
  }
  z[j] = 0;
}

/* Check to see if the string is of the form:  "TAG = VALUE" with optional
** whitespace before and around tokens.  If it is, return a pointer to the
** first character of VALUE.  If it is not, return NULL.
*/
static const char *csv_parameter(const char *zTag, int nTag, const char *z){
  z = csv_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = csv_skip_whitespace(z+nTag);
  if( z[0]!='=' ) return 0;
  return csv_skip_whitespace(z+1);
}

/* Decode a parameter that requires a dequoted string.
**
** Return 1 if the parameter is seen, or 0 if not.  1 is returned
** even if there is an error.  If an error occurs, then an error message
** is left in p->zErr.  If there are no errors, p->zErr[0]==0.
*/
static int csv_string_parameter(
  StreamReader *p,            /* Leave the error message here, if there is one */
  const char *zParam,      /* Parameter we are checking for */
  const char *zArg,        /* Raw text of the virtual table argment */
  char **pzVal             /* Write the dequoted string value here */
){
  const char *zValue;
  zValue = csv_parameter(zParam,(int)strlen(zParam),zArg);
  if( zValue==0 ) return 0;
  p->zErr[0] = 0;
  if( *pzVal ){
    csv_errmsg(p, "more than one '%s' parameter", zParam);
    return 1;
  }
  *pzVal = sqlite3_mprintf("%s", zValue);
  if( *pzVal==0 ){
    csv_errmsg(p, "out of memory");
    return 1;
  }
  csv_trim_whitespace(*pzVal);
  csv_dequote(*pzVal);
  return 1;
}


/* Return 0 if the argument is false and 1 if it is true.  Return -1 if
** we cannot really tell.
*/
static int csv_boolean(const char *z){
  if( sqlite3_stricmp("yes",z)==0
   || sqlite3_stricmp("on",z)==0
   || sqlite3_stricmp("true",z)==0
   || (z[0]=='1' && z[1]==0)
  ){
    return 1;
  }
  if( sqlite3_stricmp("no",z)==0
   || sqlite3_stricmp("off",z)==0
   || sqlite3_stricmp("false",z)==0
   || (z[0]=='0' && z[1]==0)
  ){
    return 0;
  }
  return -1;
}

/* Check to see if the string is of the form:  "TAG = BOOLEAN" or just "TAG".
** If it is, set *pValue to be the value of the boolean ("true" if there is
** not "= BOOLEAN" component) and return non-zero.  If the input string
** does not begin with TAG, return zero.
*/
static int csv_boolean_parameter(
  const char *zTag,       /* Tag we are looking for */
  int nTag,               /* Size of the tag in bytes */
  const char *z,          /* Input parameter */
  int *pValue             /* Write boolean value here */
){
  int b;
  z = csv_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = csv_skip_whitespace(z + nTag);
  if( z[0]==0 ){
    *pValue = 1;
    return 1;
  }
  if( z[0]!='=' ) return 0;
  z = csv_skip_whitespace(z+1);
  b = csv_boolean(z);
  if( b>=0 ){
    *pValue = b;
    return 1;
  }
  return 0;
}

static int srd_parse_until(const char * str, const char *expr, int * pos)
{
  int p = *pos;
  //char * debug = &str[p];
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
  //char * debug = &str[p];

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

static int srd_Connect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  srdTable *pTable = 0;     /* The srdTable object to construct */  
  int rc = SQLITE_OK;     /* Result code from this routine */
  int i;                  /* Loop counters */      
  std::string schema;
  StreamReaderParam * param = (StreamReaderParam*)pAux;
    
  pTable =(srdTable *) sqlite3_malloc( sizeof(srdTable) );
  *ppVtab = (sqlite3_vtab*)pTable;
  if( pTable==0 ) {
    goto tbltab_connect_oom;
  }

  memset(pTable, 0, sizeof(srdTable));
  
  for(i = 0; i < argc; i++) {
      std::cout << i << " " << argv[i] << std::endl;
  }

  schema = std::string("CREATE TABLE S3Object (") +  param->schema + ")";

  std::cout << "schema" << " " << schema << std::endl;
  
  rc = srd_parse_schema(schema.c_str(), pTable);
  if( rc ) {
    std::cerr << "BAD schema" << " " << schema << std::endl;
    goto tbltab_connect_error;
  }  

  pTable->in = param->in;
  
  rc = sqlite3_declare_vtab(db, schema.c_str());
  if( rc ){
    std::cerr << "Bad schema: " <<  schema << " - " << sqlite3_errmsg(db) << std::endl;
    goto tbltab_connect_error;
  }

  /* Rationale for DIRECTONLY:
  ** An attacker who controls a database schema could use this vtab
  ** to exfiltrate sensitive data from other files in the filesystem.
  ** And, recommended practice is to put all virtual tables in the
  ** TEMP namespace, so they should still be usable from within TEMP
  ** views, so there shouldn't be a serious loss of functionality by
  ** prohibiting the use of this vtab from persistent triggers and views.
  */
  sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);

  return SQLITE_OK;

tbltab_connect_oom:
  rc = SQLITE_NOMEM;
  std::cerr << "out of memory" << std::endl;

tbltab_connect_error:
  if( pTable ) {
    srd_Disconnect(&pTable->base);
  }

  return SQLITE_ERROR;
}

/*
** Reset the current row content held by a srdCursor.
*/
static void srd_CursorRowReset(srdCursor *pCur){
  srdTable *pTab = (srdTable*)pCur->base.pVtab;
  int i;
  for(i=0; i<pTab->nCol; i++){
    sqlite3_free(pCur->azVal[i]);
    pCur->azVal[i] = 0;
    pCur->aLen[i] = 0;
    pCur->azPtr[i] = 0;
  }
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int srd_Create(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
 return srd_Connect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a srdCursor.
*/
static int srd_Close(sqlite3_vtab_cursor *cur){
  srdCursor *pCur = (srdCursor*)cur;
  srd_CursorRowReset(pCur);
  //csv_reader_reset(&pCur->rdr);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new srdTable cursor object.
*/
static int srd_Open(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  srdTable *pTab = (srdTable*)p;
  srdCursor *pCur;
  size_t nByte;
  nByte = sizeof(*pCur) + (sizeof(char*)+sizeof(int)+sizeof(char*))*pTab->nCol;
  pCur = (srdCursor *)sqlite3_malloc64( nByte );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, nByte);
  pCur->azVal = (char**)&pCur[1];
  pCur->aLen = (int*)&pCur->azVal[pTab->nCol];
  pCur->azPtr = (char**)&pCur->aLen[pTab->nCol];
  *ppCursor = &pCur->base;
  if( srd_reader_open(&pCur->rdr, pTab->in) ){
    csv_xfer_error(pTab, &pCur->rdr);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int tbl_get_field_indecies(srdCursor *pCur, int nCol, int delim)
{
  int c;
  int i = 0;
  StreamReader *p = &pCur->rdr;
  char * azPtr;
  char * buf;
  char * end;
  int iIn = 0;

  if(CSV_UNLIKELY(p->nIn == 0)) {
    csv_getc_refill(p);
    buf = &p->zIn[0];
  } else {
    buf = &p->zIn[p->iIn];
    iIn++;
  }
    
  end = &p->zIn[p->nIn-1];
  do {
    //c = csv_getc(p);
    azPtr = buf;
    c = *buf;
    buf++;
    iIn++;
    if(buf >= end){ return SQLITE_IOERR; }

    if( CSV_UNLIKELY(c==EOF || c=='"') ){
        return SQLITE_IOERR;
    }

    while(c!=delim && c!='\n' && c!='"' && c!=EOF){      
      //c = csv_getc(p);
      //c = ((unsigned char*)p->zIn)[iIn-1];
      c = *buf;
      buf++;
      iIn++;
      //if(iIn >= nIn){ return SQLITE_IOERR; }
      if(buf >= end){ return SQLITE_IOERR; }
    }
    if( c==delim ){
      *(buf - 1) = 0;
      pCur->azPtr[i] = azPtr;
      azPtr = buf;
      i++;
    } else {
        return SQLITE_IOERR;
    }
  } while(i<nCol);

  if(*buf=='\n'){
    p->iIn += iIn;
  }

  return SQLITE_OK;
}

/*
** Advance a srdCursor to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int srd_Next(sqlite3_vtab_cursor *cur){
  srdCursor *pCur = (srdCursor*)cur;
  srdTable *pTab = (srdTable*)cur->pVtab;
  int i = 0;
  char *z;

  #if 1
  int rc;

  /* Evaluate if we can use inplace pointers */ 
  rc = tbl_get_field_indecies(pCur, pTab->nCol, FDELIM);
  if(rc == SQLITE_OK){
    pCur->iRowid++;
    return SQLITE_OK;
  }

  for(i = 0; i < pTab->nCol; i++){
    pCur->azPtr[i] = 0;
  }
  #endif

  i = 0;

  do{
    z = tbl_read_one_field(&pCur->rdr,FDELIM);
    //if( z==0 ){ break; }
    if(pCur->rdr.cTerm==EOF) { break; }

    if( i<pTab->nCol ){
      if( CSV_UNLIKELY(pCur->aLen[i] < pCur->rdr.n+1 )){
        char *zNew = (char *)sqlite3_realloc64(pCur->azVal[i], pCur->rdr.n+1);
        if( zNew==0 ){
          csv_errmsg(&pCur->rdr, "out of memory");
          csv_xfer_error(pTab, &pCur->rdr);
          break;
        }
        pCur->azVal[i] = zNew;
        pCur->aLen[i] = pCur->rdr.n+1;
      }
      memcpy(pCur->azVal[i], z, pCur->rdr.n+1);
      i++;
    }
  }while( pCur->rdr.cTerm!='\n' ); // while( pCur->rdr.cTerm==FDELIM );

  if( /* z==0 || */ (pCur->rdr.cTerm==EOF && i<pTab->nCol) ){
    pCur->iRowid = -1;
  }else{
    pCur->iRowid++;
    while( i<pTab->nCol ){
      sqlite3_free(pCur->azVal[i]);
      pCur->azVal[i] = 0;
      pCur->aLen[i] = 0;
      pCur->azPtr[i] = 0;
      i++;
    }
  }
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the srdCursor
** is currently pointing.
*/
static int srd_Column(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  srdCursor *pCur = (srdCursor*)cur;
  srdTable *pTab = (srdTable*)cur->pVtab;
  if( i>=0 && i<pTab->nCol){
    if(pCur->azPtr[i]!=0){
      if(pTab->cTypes[i] == SQLITE_AFF_INTEGER){        
        sqlite3_result_int(ctx, sqlite3_atoi(pCur->azPtr[i]));
        //sqlite3_result_int(ctx, atoi(pCur->azPtr[i]));
        //sqlite3_result_text(ctx, pCur->azPtr[i], -1 , SQLITE_STATIC);
      } else {      
        sqlite3_result_text(ctx, pCur->azPtr[i], -1 , SQLITE_TRANSIENT);
      }      
    } else if (pCur->azVal[i]!=0){
      if(pTab->cTypes[i]== SQLITE_AFF_INTEGER){
        sqlite3_result_int(ctx, sqlite3_atoi(pCur->azVal[i]));
        //sqlite3_result_int(ctx, atoi(pCur->azVal[i]));
        //sqlite3_result_text(ctx, pCur->azVal[i], -1 /* pCur->aLen[i] */, SQLITE_STATIC);
      } else {      
        sqlite3_result_text(ctx, pCur->azVal[i], -1 /* pCur->aLen[i] */, SQLITE_STATIC);        
      }
    }
  }

  /*
  if( i>=0 && i<pTab->nCol && pCur->azVal[i]!=0 ){
    if(pTab->cTypes[i]){
      sqlite3_result_int(ctx, atoi(pCur->azVal[i]));
    } else {      
      sqlite3_result_text(ctx, pCur->azVal[i], -1 , SQLITE_STATIC);
      //sqlite3_result_text(ctx, pCur->azVal[i], pCur->aLen[i] -1, SQLITE_STATIC);
    }
  }
  */
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
  return pCur->iRowid<0;
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
static int srd_BestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
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

int sqlite3_srd_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "StreamReader", &StreamReaderModule, 0);
  return rc;
}

int StreamReaderInit(sqlite3 *db, StreamReaderParam * param)
{
  int rc;
  SQLITE_EXTENSION_INIT2(NULL);
  rc = sqlite3_create_module(db, "StreamReader", &StreamReaderModule, param);
  return rc;
}
