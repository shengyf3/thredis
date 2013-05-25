
//#include "sqliteInt.h"
#include "redis.h"
#include "sqlite3.h"
#include <assert.h>

typedef struct Rcache Rcache;
typedef struct PgHdr1 PgHdr1;
typedef struct PgFreeslot PgFreeslot;
typedef struct PGroup PGroup;

/* Each page cache (or PCache) belongs to a PGroup.  A PGroup is a set 
** of one or more PCaches that are able to recycle each others unpinned
** pages when they are under memory pressure.  A PGroup is an instance of
** the following object.
**
** This page cache implementation works in one of two modes:
**
**   (1)  Every PCache is the sole member of its own PGroup.  There is
**        one PGroup per PCache.
**
**   (2)  There is a single global PGroup that all PCaches are a member
**        of.
**
** Mode 1 uses more memory (since PCache instances are not able to rob
** unused pages from other PCaches) but it also operates without a mutex,
** and is therefore often faster.  Mode 2 requires a mutex in order to be
** threadsafe, but recycles pages more efficiently.
**
** For mode (1), PGroup.mutex is NULL.  For mode (2) there is only a single
** PGroup which is the rcache.grp global variable and its mutex is
** SQLITE_MUTEX_STATIC_LRU.
*/
struct PGroup {
  sqlite3_mutex *mutex;          /* MUTEX_STATIC_LRU or NULL */
  unsigned int nMaxPage;         /* Sum of nMax for purgeable caches */
  unsigned int nMinPage;         /* Sum of nMin for purgeable caches */
  unsigned int mxPinned;         /* nMaxpage + 10 - nMinPage */
  unsigned int nCurrentPage;     /* Number of purgeable pages allocated */
  PgHdr1 *pLruHead, *pLruTail;   /* LRU list of unpinned pages */
};

/* Each page cache is an instance of the following object.  Every
** open database file (including each in-memory database and each
** temporary or transient database) has a single page cache which
** is an instance of this object.
**
** Pointers to structures of this type are cast and returned as 
** opaque sqlite3_pcache* handles.
*/
struct Rcache {
  /* Cache configuration parameters. Page size (szPage) and the purgeable
  ** flag (bPurgeable) are set when the cache is created. nMax may be 
  ** modified at any time by a call to the rcacheCachesize() method.
  ** The PGroup mutex must be held when accessing nMax.
  */
  PGroup *pGroup;                     /* PGroup this cache belongs to */
  int szPage;                         /* Size of allocated pages in bytes */
  int szExtra;                        /* Size of extra space in bytes */
  int bPurgeable;                     /* True if cache is purgeable */
  unsigned int nMin;                  /* Minimum number of pages reserved */
  unsigned int nMax;                  /* Configured "cache_size" value */
  unsigned int n90pct;                /* nMax*9/10 */
  unsigned int iMaxKey;               /* Largest key seen since xTruncate() */

  /* Hash table of all pages. The following variables may only be accessed
  ** when the accessor is holding the PGroup mutex.
  */
  unsigned int nRecyclable;           /* Number of pages in the LRU list */
  unsigned int nPage;                 /* Total number of pages in apHash */
  unsigned int nHash;                 /* Number of slots in apHash[] */
  PgHdr1 **apHash;                    /* Hash table for fast lookup by key */
};

/*
** Each cache entry is represented by an instance of the following 
** structure. Unless SQLITE_PCACHE_SEPARATE_HEADER is defined, a buffer of
** PgHdr1.pCache->szPage bytes is allocated directly before this structure 
** in memory.
*/
struct PgHdr1 {
  sqlite3_pcache_page page;
  unsigned int iKey;             /* Key value (page number) */
  PgHdr1 *pNext;                 /* Next in hash table chain */
  Rcache *pCache;               /* Cache that currently owns this page */
  PgHdr1 *pLruNext;              /* Next in LRU list of unpinned pages */
  PgHdr1 *pLruPrev;              /* Previous in LRU list of unpinned pages */
};

/*
** Free slots in the allocator used to divide up the buffer provided using
** the SQLITE_CONFIG_PAGECACHE mechanism.
*/
struct PgFreeslot {
  PgFreeslot *pNext;  /* Next free slot */
};

/*
** Global data used by this cache.
*/
static struct PCacheGlobal {
  PGroup grp;                    /* The global PGroup for mode (2) */

  /* Variables related to SQLITE_CONFIG_PAGECACHE settings.  The
  ** szSlot, nSlot, pStart, pEnd, nReserve, and isInit values are all
  ** fixed at sqlite3_initialize() time and do not require mutex protection.
  ** The nFreeSlot and pFree values do require mutex protection.
  */
  int isInit;                    /* True if initialized */
  int szSlot;                    /* Size of each free slot */
  int nSlot;                     /* The number of pcache slots */
  int nReserve;                  /* Try to keep nFreeSlot above this */
  void *pStart, *pEnd;           /* Bounds of pagecache malloc range */
  /* Above requires no mutex.  Use mutex below for variable that follow. */
  sqlite3_mutex *mutex;          /* Mutex for accessing the following: */
  PgFreeslot *pFree;             /* Free page blocks */
  int nFreeSlot;                 /* Number of unused pcache slots */
  /* The following value requires a mutex to change.  We skip the mutex on
  ** reading because (1) most platforms read a 32-bit integer atomically and
  ** (2) even if an incorrect value is read, no great harm is done since this
  ** is really just an optimization. */
  int bUnderPressure;            /* True if low on PAGECACHE memory */
} rcache_g;

/*
** All code in this file should access the global structure above via the
** alias "rcache". This ensures that the WSD emulation is used when
** compiling for systems that do not support real WSD.
*/
#define GLOBAL(t,v) v //ZZZ
#define rcache (GLOBAL(struct PCacheGlobal, rcache_g))

/*
** Macros to enter and leave the PCache LRU mutex.
*/
#define rcacheEnterMutex(X) sqlite3_mutex_enter((X)->mutex)
#define rcacheLeaveMutex(X) sqlite3_mutex_leave((X)->mutex)

/******************************************************************************/
/******** Page Allocation/SQLITE_CONFIG_PCACHE Related Functions **************/

/*
** This function is called during initialization if a static buffer is 
** supplied to use for the page-cache by passing the SQLITE_CONFIG_PAGECACHE
** verb to sqlite3_config(). Parameter pBuf points to an allocation large
** enough to contain 'n' buffers of 'sz' bytes each.
**
** This routine is called from sqlite3_initialize() and so it is guaranteed
** to be serialized already.  There is no need for further mutexing.
*/
#define ROUNDDOWN8(x) ((x)&~7) //ZZZ
void sqlite3PCacheBufferSetup(void *pBuf, int sz, int n){
  if( rcache.isInit ){
    PgFreeslot *p;
    sz = ROUNDDOWN8(sz);
    rcache.szSlot = sz;
    rcache.nSlot = rcache.nFreeSlot = n;
    rcache.nReserve = n>90 ? 10 : (n/10 + 1);
    rcache.pStart = pBuf;
    rcache.pFree = 0;
    rcache.bUnderPressure = 0;
    while( n-- ){
      p = (PgFreeslot*)pBuf;
      p->pNext = rcache.pFree;
      rcache.pFree = p;
      pBuf = (void*)&((char*)pBuf)[sz];
    }
    rcache.pEnd = pBuf;
  }
}

/*
** Malloc function used within this file to allocate space from the buffer
** configured using sqlite3_config(SQLITE_CONFIG_PAGECACHE) option. If no 
** such buffer exists or there is no space left in it, this function falls 
** back to sqlite3Malloc().
**
** Multiple threads can run this routine at the same time.  Global variables
** in rcache need to be protected via mutex.
*/
static void *rcacheAlloc(int nByte){
  void *p = 0;
//  assert( sqlite3_mutex_notheld(rcache.grp.mutex) );
//  sqlite3StatusSet(SQLITE_STATUS_PAGECACHE_SIZE, nByte);
  if( nByte<=rcache.szSlot ){
    sqlite3_mutex_enter(rcache.mutex);
    p = (PgHdr1 *)rcache.pFree;
    if( p ){
      rcache.pFree = rcache.pFree->pNext;
      rcache.nFreeSlot--;
      rcache.bUnderPressure = rcache.nFreeSlot<rcache.nReserve;
      assert( rcache.nFreeSlot>=0 );
//      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, 1);
    }
    sqlite3_mutex_leave(rcache.mutex);
  }
  if( p==0 ){
    /* Memory is not available in the SQLITE_CONFIG_PAGECACHE pool.  Get
    ** it from sqlite3Malloc instead.
    */
//    p = sqlite3Malloc(nByte);
      p = zmalloc(nByte); // ZZZ
#ifndef SQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
    if( p ){
//      int sz = sqlite3MallocSize(p);
      sqlite3_mutex_enter(rcache.mutex);
//      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, sz);
      sqlite3_mutex_leave(rcache.mutex);
    }
#endif
//    sqlite3MemdebugSetType(p, MEMTYPE_PCACHE);
  }
  return p;
}

/*
** Free an allocated buffer obtained from rcacheAlloc().
*/
static int rcacheFree(void *p){
  int nFreed = 0;
  if( p==0 ) return 0;
  if( p>=rcache.pStart && p<rcache.pEnd ){
    PgFreeslot *pSlot;
    sqlite3_mutex_enter(rcache.mutex);
//    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, -1);
    pSlot = (PgFreeslot*)p;
    pSlot->pNext = rcache.pFree;
    rcache.pFree = pSlot;
    rcache.nFreeSlot++;
    rcache.bUnderPressure = rcache.nFreeSlot<rcache.nReserve;
    assert( rcache.nFreeSlot<=rcache.nSlot );
    sqlite3_mutex_leave(rcache.mutex);
  }else{
//    assert( sqlite3MemdebugHasType(p, MEMTYPE_PCACHE) );
//    sqlite3MemdebugSetType(p, MEMTYPE_HEAP);
//    nFreed = sqlite3MallocSize(p);
#ifndef SQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
    sqlite3_mutex_enter(rcache.mutex);
//    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, -nFreed);
    sqlite3_mutex_leave(rcache.mutex);
#endif
    sqlite3_free(p);
  }
  return nFreed;
}

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** Return the size of a pcache allocation
*/
static int rcacheMemSize(void *p){
  if( p>=rcache.pStart && p<rcache.pEnd ){
    return rcache.szSlot;
  }else{
      int iSize = 0; //ZZZ
//    assert( sqlite3MemdebugHasType(p, MEMTYPE_PCACHE) );
//    sqlite3MemdebugSetType(p, MEMTYPE_HEAP);
//    iSize = sqlite3MallocSize(p);
//    sqlite3MemdebugSetType(p, MEMTYPE_PCACHE);
    return iSize;
  }
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

/*
** Allocate a new page object initially associated with cache pCache.
*/
typedef uint8_t u8;             /* 1-byte unsigned integer */ //ZZZ

static PgHdr1 *rcacheAllocPage(Rcache *pCache){
  PgHdr1 *p = 0;
  void *pPg;

  /* The group mutex must be released before rcacheAlloc() is called. This
  ** is because it may call sqlite3_release_memory(), which assumes that 
  ** this mutex is not held. */
//  assert( sqlite3_mutex_held(pCache->pGroup->mutex) );
  rcacheLeaveMutex(pCache->pGroup);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
  pPg = rcacheAlloc(pCache->szPage);
//  p = sqlite3Malloc(sizeof(PgHdr1) + pCache->szExtra);
  p = zmalloc(sizeof(PgHdr1) + pCache->szExtra); //ZZZ
  if( !pPg || !p ){
    rcacheFree(pPg);
    sqlite3_free(p);
    pPg = 0;
  }
#else
  pPg = rcacheAlloc(sizeof(PgHdr1) + pCache->szPage + pCache->szExtra);
  p = (PgHdr1 *)&((u8 *)pPg)[pCache->szPage];
#endif
  rcacheEnterMutex(pCache->pGroup);

  if( pPg ){
    p->page.pBuf = pPg;
    p->page.pExtra = &p[1];
    if( pCache->bPurgeable ){
      pCache->pGroup->nCurrentPage++;
    }
    return p;
  }
  return 0;
}

/*
** Free a page object allocated by rcacheAllocPage().
**
** The pointer is allowed to be NULL, which is prudent.  But it turns out
** that the current implementation happens to never call this routine
** with a NULL pointer, so we mark the NULL test with ALWAYS().
*/
#define ALWAYS(X)      (X) //ZZZ
static void rcacheFreePage(PgHdr1 *p){
  if( ALWAYS(p) ){
    Rcache *pCache = p->pCache;
//    assert( sqlite3_mutex_held(p->pCache->pGroup->mutex) );
    rcacheFree(p->page.pBuf);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
    sqlite3_free(p);
#endif
    if( pCache->bPurgeable ){
      pCache->pGroup->nCurrentPage--;
    }
  }
}

/*
** Malloc function used by SQLite to obtain space from the buffer configured
** using sqlite3_config(SQLITE_CONFIG_PAGECACHE) option. If no such buffer
** exists, this function falls back to sqlite3Malloc().
*/
void *sqlite3PageMalloc(int sz){
  return rcacheAlloc(sz);
}

/*
** Free an allocated buffer obtained from sqlite3PageMalloc().
*/
void sqlite3PageFree(void *p){
  rcacheFree(p);
}


/*
** Return true if it desirable to avoid allocating a new page cache
** entry.
**
** If memory was allocated specifically to the page cache using
** SQLITE_CONFIG_PAGECACHE but that memory has all been used, then
** it is desirable to avoid allocating a new page cache entry because
** presumably SQLITE_CONFIG_PAGECACHE was suppose to be sufficient
** for all page cache needs and we should not need to spill the
** allocation onto the heap.
**
** Or, the heap is used for all page cache memory but the heap is
** under memory pressure, then again it is desirable to avoid
** allocating a new page cache entry in order to avoid stressing
** the heap even further.
*/
static int rcacheUnderMemoryPressure(Rcache *pCache){
  if( rcache.nSlot && (pCache->szPage+pCache->szExtra)<=rcache.szSlot ){
    return rcache.bUnderPressure;
  }else{
//    return sqlite3HeapNearlyFull();
      return 0; //ZZZ
  }
}

/******************************************************************************/
/******** General Implementation Functions ************************************/

/*
** This function is used to resize the hash table used by the cache passed
** as the first argument.
**
** The PCache mutex must be held when this function is called.
*/
static int rcacheResizeHash(Rcache *p){
  PgHdr1 **apNew;
  unsigned int nNew;
  unsigned int i;

//  assert( sqlite3_mutex_held(p->pGroup->mutex) );

  nNew = p->nHash*2;
  if( nNew<256 ){
    nNew = 256;
  }

  rcacheLeaveMutex(p->pGroup);
//  if( p->nHash ){ sqlite3BeginBenignMalloc(); }
//  apNew = (PgHdr1 **)sqlite3MallocZero(sizeof(PgHdr1 *)*nNew);
  apNew = (PgHdr1 **)zcalloc(sizeof(PgHdr1 *)*nNew);
//  if( p->nHash ){ sqlite3EndBenignMalloc(); }
  rcacheEnterMutex(p->pGroup);
  if( apNew ){
    for(i=0; i<p->nHash; i++){
      PgHdr1 *pPage;
      PgHdr1 *pNext = p->apHash[i];
      while( (pPage = pNext)!=0 ){
        unsigned int h = pPage->iKey % nNew;
        pNext = pPage->pNext;
        pPage->pNext = apNew[h];
        apNew[h] = pPage;
      }
    }
    sqlite3_free(p->apHash);
    p->apHash = apNew;
    p->nHash = nNew;
  }

  return (p->apHash ? SQLITE_OK : SQLITE_NOMEM);
}

/*
** This function is used internally to remove the page pPage from the 
** PGroup LRU list, if is part of it. If pPage is not part of the PGroup
** LRU list, then this function is a no-op.
**
** The PGroup mutex must be held when this function is called.
**
** If pPage is NULL then this routine is a no-op.
*/
static void rcachePinPage(PgHdr1 *pPage){
  Rcache *pCache;
  PGroup *pGroup;

  if( pPage==0 ) return;
  pCache = pPage->pCache;
  pGroup = pCache->pGroup;
//  assert( sqlite3_mutex_held(pGroup->mutex) );
  if( pPage->pLruNext || pPage==pGroup->pLruTail ){
    if( pPage->pLruPrev ){
      pPage->pLruPrev->pLruNext = pPage->pLruNext;
    }
    if( pPage->pLruNext ){
      pPage->pLruNext->pLruPrev = pPage->pLruPrev;
    }
    if( pGroup->pLruHead==pPage ){
      pGroup->pLruHead = pPage->pLruNext;
    }
    if( pGroup->pLruTail==pPage ){
      pGroup->pLruTail = pPage->pLruPrev;
    }
    pPage->pLruNext = 0;
    pPage->pLruPrev = 0;
    pPage->pCache->nRecyclable--;
  }
}


/*
** Remove the page supplied as an argument from the hash table 
** (Rcache.apHash structure) that it is currently stored in.
**
** The PGroup mutex must be held when this function is called.
*/
static void rcacheRemoveFromHash(PgHdr1 *pPage){
  unsigned int h;
  Rcache *pCache = pPage->pCache;
  PgHdr1 **pp;

//  assert( sqlite3_mutex_held(pCache->pGroup->mutex) );
  h = pPage->iKey % pCache->nHash;
  for(pp=&pCache->apHash[h]; (*pp)!=pPage; pp=&(*pp)->pNext);
  *pp = (*pp)->pNext;

  pCache->nPage--;
}

/*
** If there are currently more than nMaxPage pages allocated, try
** to recycle pages to reduce the number allocated to nMaxPage.
*/
static void rcacheEnforceMaxPage(PGroup *pGroup){
//  assert( sqlite3_mutex_held(pGroup->mutex) );
  while( pGroup->nCurrentPage>pGroup->nMaxPage && pGroup->pLruTail ){
    PgHdr1 *p = pGroup->pLruTail;
    assert( p->pCache->pGroup==pGroup );
    rcachePinPage(p);
    rcacheRemoveFromHash(p);
    rcacheFreePage(p);
  }
}

/*
** Discard all pages from cache pCache with a page number (key value) 
** greater than or equal to iLimit. Any pinned pages that meet this 
** criteria are unpinned before they are discarded.
**
** The PCache mutex must be held when this function is called.
*/
#define TESTONLY(X) //ZZZ
static void rcacheTruncateUnsafe(
  Rcache *pCache,             /* The cache to truncate */
  unsigned int iLimit          /* Drop pages with this pgno or larger */
){
  TESTONLY( unsigned int nPage = 0; )  /* To assert pCache->nPage is correct */
  unsigned int h;
//  assert( sqlite3_mutex_held(pCache->pGroup->mutex) );
  for(h=0; h<pCache->nHash; h++){
    PgHdr1 **pp = &pCache->apHash[h]; 
    PgHdr1 *pPage;
    while( (pPage = *pp)!=0 ){
      if( pPage->iKey>=iLimit ){
        pCache->nPage--;
        *pp = pPage->pNext;
        rcachePinPage(pPage);
        rcacheFreePage(pPage);
      }else{
        pp = &pPage->pNext;
        TESTONLY( nPage++; )
      }
    }
  }
//  assert( pCache->nPage==nPage );
}

/******************************************************************************/
/******** sqlite3_pcache Methods **********************************************/

/*
** Implementation of the sqlite3_pcache.xInit method.
*/
#define UNUSED_PARAMETER(x) (void)(x) //ZZZ
int rcacheInit(void *NotUsed){
    printf("ZZZ in xInit()\n");
  UNUSED_PARAMETER(NotUsed);
  assert( rcache.isInit==0 );
  memset(&rcache, 0, sizeof(rcache));
//  if( sqlite3GlobalConfig.bCoreMutex ){
    rcache.grp.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_LRU);
    rcache.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_PMEM);
//  }
  rcache.grp.mxPinned = 10;
  rcache.isInit = 1;
  return SQLITE_OK;
}

/*
** Implementation of the sqlite3_pcache.xShutdown method.
** Note that the static mutex allocated in xInit does 
** not need to be freed.
*/
void rcacheShutdown(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( rcache.isInit!=0 );
  memset(&rcache, 0, sizeof(rcache));
}

/*
** Implementation of the sqlite3_pcache.xCreate method.
**
** Allocate a new cache.
*/
sqlite3_pcache *rcacheCreate(int szPage, int szExtra, int bPurgeable){
  Rcache *pCache;      /* The newly created page cache */
  PGroup *pGroup;       /* The group the new page cache will belong to */
  int sz;               /* Bytes of memory required to allocate the new cache */

  /*
  ** The seperateCache variable is true if each PCache has its own private
  ** PGroup.  In other words, separateCache is true for mode (1) where no
  ** mutexing is required.
  **
  **   *  Always use a unified cache (mode-2) if ENABLE_MEMORY_MANAGEMENT
  **
  **   *  Always use a unified cache in single-threaded applications
  **
  **   *  Otherwise (if multi-threaded and ENABLE_MEMORY_MANAGEMENT is off)
  **      use separate caches (mode-1)
  */
#if defined(SQLITE_ENABLE_MEMORY_MANAGEMENT) || SQLITE_THREADSAFE==0
  const int separateCache = 0;
#else
  int separateCache = sqlite3GlobalConfig.bCoreMutex>0;
#endif
  printf("ZZZ in xCreate(), szPage: %d, szExtra: %d, bPurgeable: %d\n", szPage, szExtra, bPurgeable);
  assert( (szPage & (szPage-1))==0 && szPage>=512 && szPage<=65536 );
  assert( szExtra < 300 );

  sz = sizeof(Rcache) + sizeof(PGroup)*separateCache;
//  pCache = (Rcache *)sqlite3MallocZero(sz);
  pCache = (Rcache *)zcalloc(sz);
  if( pCache ){
    if( separateCache ){
      pGroup = (PGroup*)&pCache[1];
      pGroup->mxPinned = 10;
    }else{
      pGroup = &rcache.grp;
    }
    pCache->pGroup = pGroup;
    pCache->szPage = szPage;
    pCache->szExtra = szExtra;
    pCache->bPurgeable = (bPurgeable ? 1 : 0);
    if( bPurgeable ){
      pCache->nMin = 10;
      rcacheEnterMutex(pGroup);
      pGroup->nMinPage += pCache->nMin;
      pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
      rcacheLeaveMutex(pGroup);
    }
  }
  return (sqlite3_pcache *)pCache;
}

/*
** Implementation of the sqlite3_pcache.xCachesize method. 
**
** Configure the cache_size limit for a cache.
*/
void rcacheCachesize(sqlite3_pcache *p, int nMax){
  Rcache *pCache = (Rcache *)p;
  printf("ZZZ in xCachesize(), p: %d, nMax: %d\n", (int)p, nMax);
  if( pCache->bPurgeable ){
    PGroup *pGroup = pCache->pGroup;
    rcacheEnterMutex(pGroup);
    pGroup->nMaxPage += (nMax - pCache->nMax);
    pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
    pCache->nMax = nMax;
    pCache->n90pct = pCache->nMax*9/10;
    rcacheEnforceMaxPage(pGroup);
    rcacheLeaveMutex(pGroup);
  }
}

/*
** Implementation of the sqlite3_pcache.xShrink method. 
**
** Free up as much memory as possible.
*/
void rcacheShrink(sqlite3_pcache *p){
  Rcache *pCache = (Rcache*)p;
    printf("ZZZ in xShrink()\n");
  if( pCache->bPurgeable ){
    PGroup *pGroup = pCache->pGroup;
    int savedMaxPage;
    rcacheEnterMutex(pGroup);
    savedMaxPage = pGroup->nMaxPage;
    pGroup->nMaxPage = 0;
    rcacheEnforceMaxPage(pGroup);
    pGroup->nMaxPage = savedMaxPage;
    rcacheLeaveMutex(pGroup);
  }
}

/*
** Implementation of the sqlite3_pcache.xPagecount method. 
*/
int rcachePagecount(sqlite3_pcache *p){
  int n;
  Rcache *pCache = (Rcache*)p;
    printf("ZZZ in xPagecount()\n");
  rcacheEnterMutex(pCache->pGroup);
  n = pCache->nPage;
  rcacheLeaveMutex(pCache->pGroup);
  return n;
}

/*
** Implementation of the sqlite3_pcache.xFetch method. 
**
** Fetch a page by key value.
**
** Whether or not a new page may be allocated by this function depends on
** the value of the createFlag argument.  0 means do not allocate a new
** page.  1 means allocate a new page if space is easily available.  2 
** means to try really hard to allocate a new page.
**
** For a non-purgeable cache (a cache used as the storage for an in-memory
** database) there is really no difference between createFlag 1 and 2.  So
** the calling function (pcache.c) will never have a createFlag of 1 on
** a non-purgeable cache.
**
** There are three different approaches to obtaining space for a page,
** depending on the value of parameter createFlag (which may be 0, 1 or 2).
**
**   1. Regardless of the value of createFlag, the cache is searched for a 
**      copy of the requested page. If one is found, it is returned.
**
**   2. If createFlag==0 and the page is not already in the cache, NULL is
**      returned.
**
**   3. If createFlag is 1, and the page is not already in the cache, then
**      return NULL (do not allocate a new page) if any of the following
**      conditions are true:
**
**       (a) the number of pages pinned by the cache is greater than
**           Rcache.nMax, or
**
**       (b) the number of pages pinned by the cache is greater than
**           the sum of nMax for all purgeable caches, less the sum of 
**           nMin for all other purgeable caches, or
**
**   4. If none of the first three conditions apply and the cache is marked
**      as purgeable, and if one of the following is true:
**
**       (a) The number of pages allocated for the cache is already 
**           Rcache.nMax, or
**
**       (b) The number of pages allocated for all purgeable caches is
**           already equal to or greater than the sum of nMax for all
**           purgeable caches,
**
**       (c) The system is under memory pressure and wants to avoid
**           unnecessary pages cache entry allocations
**
**      then attempt to recycle a page from the LRU list. If it is the right
**      size, return the recycled buffer. Otherwise, free the buffer and
**      proceed to step 5. 
**
**   5. Otherwise, allocate and return a new page buffer.
*/
sqlite3_pcache_page *rcacheFetch(
  sqlite3_pcache *p, 
  unsigned int iKey, 
  int createFlag
){
  unsigned int nPinned;
  Rcache *pCache = (Rcache *)p;
  PGroup *pGroup;
  PgHdr1 *pPage = 0;

  printf("ZZZ in xFetch() p: %d, iKey: %d, createFlad: %d\n", (int)p, (int)iKey, createFlag);
  assert( pCache->bPurgeable || createFlag!=1 );
  assert( pCache->bPurgeable || pCache->nMin==0 );
  assert( pCache->bPurgeable==0 || pCache->nMin==10 );
  assert( pCache->nMin==0 || pCache->bPurgeable );
  rcacheEnterMutex(pGroup = pCache->pGroup);

  /* Step 1: Search the hash table for an existing entry. */
  if( pCache->nHash>0 ){
    unsigned int h = iKey % pCache->nHash;
    for(pPage=pCache->apHash[h]; pPage&&pPage->iKey!=iKey; pPage=pPage->pNext);
  }

  /* Step 2: Abort if no existing page is found and createFlag is 0 */
  if( pPage || createFlag==0 ){
    rcachePinPage(pPage);
    goto fetch_out;
  }

  /* The pGroup local variable will normally be initialized by the
  ** rcacheEnterMutex() macro above.  But if SQLITE_MUTEX_OMIT is defined,
  ** then rcacheEnterMutex() is a no-op, so we have to initialize the
  ** local variable here.  Delaying the initialization of pGroup is an
  ** optimization:  The common case is to exit the module before reaching
  ** this point.
  */
#ifdef SQLITE_MUTEX_OMIT
  pGroup = pCache->pGroup;
#endif

  /* Step 3: Abort if createFlag is 1 but the cache is nearly full */
  assert( pCache->nPage >= pCache->nRecyclable );
  nPinned = pCache->nPage - pCache->nRecyclable;
  assert( pGroup->mxPinned == pGroup->nMaxPage + 10 - pGroup->nMinPage );
  assert( pCache->n90pct == pCache->nMax*9/10 );
  if( createFlag==1 && (
        nPinned>=pGroup->mxPinned
     || nPinned>=pCache->n90pct
     || rcacheUnderMemoryPressure(pCache)
  )){
    goto fetch_out;
  }

  if( pCache->nPage>=pCache->nHash && rcacheResizeHash(pCache) ){
    goto fetch_out;
  }

  /* Step 4. Try to recycle a page. */
  if( pCache->bPurgeable && pGroup->pLruTail && (
         (pCache->nPage+1>=pCache->nMax)
      || pGroup->nCurrentPage>=pGroup->nMaxPage
      || rcacheUnderMemoryPressure(pCache)
  )){
    Rcache *pOther;
    pPage = pGroup->pLruTail;
    rcacheRemoveFromHash(pPage);
    rcachePinPage(pPage);
    pOther = pPage->pCache;

    /* We want to verify that szPage and szExtra are the same for pOther
    ** and pCache.  Assert that we can verify this by comparing sums. */
    assert( (pCache->szPage & (pCache->szPage-1))==0 && pCache->szPage>=512 );
    assert( pCache->szExtra<512 );
    assert( (pOther->szPage & (pOther->szPage-1))==0 && pOther->szPage>=512 );
    assert( pOther->szExtra<512 );

    if( pOther->szPage+pOther->szExtra != pCache->szPage+pCache->szExtra ){
      rcacheFreePage(pPage);
      pPage = 0;
    }else{
      pGroup->nCurrentPage -= (pOther->bPurgeable - pCache->bPurgeable);
    }
  }

  /* Step 5. If a usable page buffer has still not been found, 
  ** attempt to allocate a new one. 
  */
  if( !pPage ){
//    if( createFlag==1 ) sqlite3BeginBenignMalloc();
    pPage = rcacheAllocPage(pCache);
//    if( createFlag==1 ) sqlite3EndBenignMalloc();
  }

  if( pPage ){
    unsigned int h = iKey % pCache->nHash;
    pCache->nPage++;
    pPage->iKey = iKey;
    pPage->pNext = pCache->apHash[h];
    pPage->pCache = pCache;
    pPage->pLruPrev = 0;
    pPage->pLruNext = 0;
    *(void **)pPage->page.pExtra = 0;
    pCache->apHash[h] = pPage;
  }

fetch_out:
  if( pPage && iKey>pCache->iMaxKey ){
    pCache->iMaxKey = iKey;
  }
  rcacheLeaveMutex(pGroup);
  return &pPage->page;
}


/*
** Implementation of the sqlite3_pcache.xUnpin method.
**
** Mark a page as unpinned (eligible for asynchronous recycling).
*/
void rcacheUnpin(
  sqlite3_pcache *p, 
  sqlite3_pcache_page *pPg, 
  int reuseUnlikely
){
  Rcache *pCache = (Rcache *)p;
  PgHdr1 *pPage = (PgHdr1 *)pPg;
  PGroup *pGroup = pCache->pGroup;

    printf("ZZZ in xUnpin()\n");
 
  assert( pPage->pCache==pCache );
  rcacheEnterMutex(pGroup);

  /* It is an error to call this function if the page is already 
  ** part of the PGroup LRU list.
  */
  assert( pPage->pLruPrev==0 && pPage->pLruNext==0 );
  assert( pGroup->pLruHead!=pPage && pGroup->pLruTail!=pPage );

  if( reuseUnlikely || pGroup->nCurrentPage>pGroup->nMaxPage ){
    rcacheRemoveFromHash(pPage);
    rcacheFreePage(pPage);
  }else{
    /* Add the page to the PGroup LRU list. */
    if( pGroup->pLruHead ){
      pGroup->pLruHead->pLruPrev = pPage;
      pPage->pLruNext = pGroup->pLruHead;
      pGroup->pLruHead = pPage;
    }else{
      pGroup->pLruTail = pPage;
      pGroup->pLruHead = pPage;
    }
    pCache->nRecyclable++;
  }

  rcacheLeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlite3_pcache.xRekey method. 
*/
void rcacheRekey(
  sqlite3_pcache *p,
  sqlite3_pcache_page *pPg,
  unsigned int iOld,
  unsigned int iNew
){
  Rcache *pCache = (Rcache *)p;
  PgHdr1 *pPage = (PgHdr1 *)pPg;
  PgHdr1 **pp;
  unsigned int h; 
    printf("ZZZ in xRekey()\n");
  assert( pPage->iKey==iOld );
  assert( pPage->pCache==pCache );

  rcacheEnterMutex(pCache->pGroup);

  h = iOld%pCache->nHash;
  pp = &pCache->apHash[h];
  while( (*pp)!=pPage ){
    pp = &(*pp)->pNext;
  }
  *pp = pPage->pNext;

  h = iNew%pCache->nHash;
  pPage->iKey = iNew;
  pPage->pNext = pCache->apHash[h];
  pCache->apHash[h] = pPage;
  if( iNew>pCache->iMaxKey ){
    pCache->iMaxKey = iNew;
  }

  rcacheLeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlite3_pcache.xTruncate method. 
**
** Discard all unpinned pages in the cache with a page number equal to
** or greater than parameter iLimit. Any pinned pages with a page number
** equal to or greater than iLimit are implicitly unpinned.
*/
void rcacheTruncate(sqlite3_pcache *p, unsigned int iLimit){
  Rcache *pCache = (Rcache *)p;
    printf("ZZZ in xTrubcate()\n");

  rcacheEnterMutex(pCache->pGroup);
  if( iLimit<=pCache->iMaxKey ){
    rcacheTruncateUnsafe(pCache, iLimit);
    pCache->iMaxKey = iLimit-1;
  }
  rcacheLeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlite3_pcache.xDestroy method. 
**
** Destroy a cache allocated using rcacheCreate().
*/
void rcacheDestroy(sqlite3_pcache *p){
  Rcache *pCache = (Rcache *)p;
  PGroup *pGroup = pCache->pGroup;
    printf("ZZZ in xDestroy()\n");
  assert( pCache->bPurgeable || (pCache->nMax==0 && pCache->nMin==0) );
  rcacheEnterMutex(pGroup);
  rcacheTruncateUnsafe(pCache, 0);
  assert( pGroup->nMaxPage >= pCache->nMax );
  pGroup->nMaxPage -= pCache->nMax;
  assert( pGroup->nMinPage >= pCache->nMin );
  pGroup->nMinPage -= pCache->nMin;
  pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
  rcacheEnforceMaxPage(pGroup);
  rcacheLeaveMutex(pGroup);
  sqlite3_free(pCache->apHash);
  sqlite3_free(pCache);
}

/*
** This function is called during initialization (sqlite3_initialize()) to
** install the default pluggable cache module, assuming the user has not
** already provided an alternative.
*/
void sqlite3PCacheSetDefault(void){
  static const sqlite3_pcache_methods2 defaultMethods = {
    1,                       /* iVersion */
    0,                       /* pArg */
    rcacheInit,             /* xInit */
    rcacheShutdown,         /* xShutdown */
    rcacheCreate,           /* xCreate */
    rcacheCachesize,        /* xCachesize */
    rcachePagecount,        /* xPagecount */
    rcacheFetch,            /* xFetch */
    rcacheUnpin,            /* xUnpin */
    rcacheRekey,            /* xRekey */
    rcacheTruncate,         /* xTruncate */
    rcacheDestroy,          /* xDestroy */
    rcacheShrink            /* xShrink */
  };
  sqlite3_config(SQLITE_CONFIG_PCACHE2, &defaultMethods);
}

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** This function is called to free superfluous dynamically allocated memory
** held by the pager system. Memory in use by any SQLite pager allocated
** by the current thread may be sqlite3_free()ed.
**
** nReq is the number of bytes of memory required. Once this much has
** been released, the function returns. The return value is the total number 
** of bytes of memory released.
*/
int sqlite3PcacheReleaseMemory(int nReq){
  int nFree = 0;
//  assert( sqlite3_mutex_notheld(rcache.grp.mutex) );
//  assert( sqlite3_mutex_notheld(rcache.mutex) );
  if( rcache.pStart==0 ){
    PgHdr1 *p;
    rcacheEnterMutex(&rcache.grp);
    while( (nReq<0 || nFree<nReq) && ((p=rcache.grp.pLruTail)!=0) ){
      nFree += rcacheMemSize(p->page.pBuf);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
      nFree += sqlite3MemSize(p);
#endif
      rcachePinPage(p);
      rcacheRemoveFromHash(p);
      rcacheFreePage(p);
    }
    rcacheLeaveMutex(&rcache.grp);
  }
  return nFree;
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

#ifdef SQLITE_TEST
/*
** This function is used by test procedures to inspect the internal state
** of the global cache.
*/
void sqlite3PcacheStats(
  int *pnCurrent,      /* OUT: Total number of pages cached */
  int *pnMax,          /* OUT: Global maximum cache size */
  int *pnMin,          /* OUT: Sum of Rcache.nMin for purgeable caches */
  int *pnRecyclable    /* OUT: Total number of pages available for recycling */
){
  PgHdr1 *p;
  int nRecyclable = 0;
  for(p=rcache.grp.pLruHead; p; p=p->pLruNext){
    nRecyclable++;
  }
  *pnCurrent = rcache.grp.nCurrentPage;
  *pnMax = (int)rcache.grp.nMaxPage;
  *pnMin = (int)rcache.grp.nMinPage;
  *pnRecyclable = nRecyclable;
}
#endif
