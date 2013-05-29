
/*
 * Redis-backed VFS. This is proof-of-concept code with many
 * imrovements needed to make it complete.
 *
 * Gregory Trubetskoy <grisha_at_apache.org> May 2013
 */

#include "sqlite3.h"
#include "redis.h"

/*
 * This is a hack because the VFS has no notion of page size. The page
 * size exists a layer above in pager.c, and the pager decides the
 * page size based on sector size as reported by the VFS.
 */

#define REDIS_VFS_SECTOR_SIZE 8192 /* Do not exceed SQLITE_MAX_DEFAULT_PAGE_SIZE */

/* our custom sqlite3_file */
typedef struct RedisVfsFile RedisVfsFile;
struct RedisVfsFile {
    sqlite3_file base;
    char *name;
    redisClient *c;
};

/* this pulls a page from a Redis hash */
static int _setPage(redisClient *c, char *key, int offset, const char *page, int size) {
    robj *o;
    int rc = SQLITE_OK;

    c->cmd = lookupCommandByCString("hset");
    c->argc = 4;
    c->argv = zmalloc(sizeof(robj*)*c->argc);
    c->argv[0] = createStringObject("hset",4);
    c->argv[1] = createStringObject(key,sdslen(key));
    c->argv[2] = createStringObjectFromLongLong(offset);
    c->argv[3] = createStringObject((char *)page,size);

    lockKey(c,c->argv[1]);
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) {
        unlockKey(c,c->argv[1]);
        rc = SQLITE_IOERR_WRITE;
        goto cleanup;
    }
    hashTypeSet(o,c->argv[2],c->argv[3]);
    unlockKey(c,c->argv[1]);

    replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);

  cleanup:
    decrRefCount(c->argv[0]);
    decrRefCount(c->argv[1]);
    decrRefCount(c->argv[2]);
    decrRefCount(c->argv[3]);
    zfree(c->argv);
    return rc;
}

static char *_getPage(redisClient *c, char *key, int offset) {
    robj *o, *p;
    char *result;

    c->cmd = lookupCommandByCString("hget");
    c->argc = 3;
    c->argv = zmalloc(sizeof(robj*)*c->argc);
    c->argv[0] = createStringObject("hget", 4);
    c->argv[1] = createStringObject(key, sdslen(key));
    c->argv[2] = createStringObjectFromLongLong(offset);

    lockKey(c,c->argv[1]);
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL ||
        (o->type != REDIS_HASH)) {
        unlockKey(c,c->argv[1]);
        result = NULL;
        goto cleanup;
    }

    p = hashTypeGetObject(o, c->argv[2]);
    if (!p) {
        unlockKey(c,c->argv[1]);
        result = NULL;
        goto cleanup;
    }
    result = p->ptr;
    decrRefCount(p);
    unlockKey(c,c->argv[1]);

    replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);

  cleanup:
    decrRefCount(c->argv[0]);
    decrRefCount(c->argv[1]);
    decrRefCount(c->argv[2]);
    zfree(c->argv);
    return result;
}

/* close a file */
static int redisVfsClose(sqlite3_file *pFile) {
    RedisVfsFile *p = (RedisVfsFile*)pFile;
    sqlite3_free(p->name);
    return SQLITE_OK;
}

/*
** Read data from a file.
*/
 static int redisVfsRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite_int64 iOfst) {
     RedisVfsFile *p = (RedisVfsFile*)pFile;
     sds key = sdsempty();

     key = sdscatprintf(key, "_sql:%s", p->name);

     if (iOfst % REDIS_VFS_SECTOR_SIZE) {
         int page_offset = iOfst % REDIS_VFS_SECTOR_SIZE;
         char *page = _getPage(p->c, key, iOfst-iOfst%REDIS_VFS_SECTOR_SIZE);
         if (!page) return SQLITE_IOERR_SHORT_READ;
         memcpy(zBuf, page+page_offset, iAmt);
     } else {
         char *page = _getPage(p->c, key, iOfst-iOfst%REDIS_VFS_SECTOR_SIZE);
         if (!page) return SQLITE_IOERR_SHORT_READ;
         memcpy(zBuf, page, iAmt);
     };

     sdsfree(key);

     return SQLITE_OK;
}

static int redisVfsWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite_int64 iOfst) {
    RedisVfsFile *p = (RedisVfsFile*)pFile;
    sds key = sdsempty();

    key = sdscatprintf(key, "_sql:%s", p->name);
    if (iOfst % REDIS_VFS_SECTOR_SIZE) {
        int page_offset = iOfst % REDIS_VFS_SECTOR_SIZE;
        char *page = _getPage(p->c, key, iOfst-iOfst % REDIS_VFS_SECTOR_SIZE);
        if (!page)
            page = zcalloc(REDIS_VFS_SECTOR_SIZE);
        memcpy(page+page_offset, zBuf, iAmt);
        _setPage(p->c, key, iOfst-iOfst%REDIS_VFS_SECTOR_SIZE, page, iAmt);
        zfree(page);
    } else {
        _setPage(p->c, key, iOfst-iOfst%REDIS_VFS_SECTOR_SIZE, zBuf, iAmt);
    };

    sdsfree(key);
    return SQLITE_OK;
}

static int redisVfsTruncate(sqlite3_file *pFile, sqlite_int64 size) {
    /* this is a noop - we don't truncate */
    return SQLITE_OK;
}

static int redisVfsSync(sqlite3_file *pFile, int flags) {
    return SQLITE_OK;
}

static int redisVfsFileSize(sqlite3_file *pFile, sqlite_int64 *pSize) {
    RedisVfsFile *p = (RedisVfsFile*)pFile;
    robj *o, *k;
    sds key = sdsempty();

    key = sdscatprintf(key, "_sql:%s", p->name);
    k = createStringObject(key,sdslen(key));
    if ((o = lookupKeyRead(p->c->db,k)) == NULL || 
        (o->type != REDIS_HASH))
        *pSize = 0;
    else
        *pSize = hashTypeLength(o) * REDIS_VFS_SECTOR_SIZE;
                                   
    sdsfree(key);
    decrRefCount(k);
    return SQLITE_OK;
}

static int redisVfsLock(sqlite3_file *pFile, int eLock) {
    return SQLITE_OK;
}
static int redisVfsUnlock(sqlite3_file *pFile, int eLock) {
    return SQLITE_OK;
}
static int redisVfsCheckReservedLock(sqlite3_file *pFile, int *pResOut) {
    *pResOut = 0;
    return SQLITE_OK;
}
static int redisVfsFileControl(sqlite3_file *pFile, int op, void *pArg) {
    return SQLITE_NOTFOUND; /* returning SQLITE_OK would be a bug! */
}
static int redisVfsSectorSize(sqlite3_file *pFile) {
    return REDIS_VFS_SECTOR_SIZE;
}
static int redisVfsDeviceCharacteristics(sqlite3_file *pFile) {
    return 0;
}

static int redisVfsOpen(sqlite3_vfs *pVfs, const char *name, sqlite3_file *pFile, int flags, int *pOutFlags) {
    static const sqlite3_io_methods redisVfs_methods = {
        1,                                /* iVersion */
        redisVfsClose,                    /* xClose */
        redisVfsRead,                     /* xRead */
        redisVfsWrite,                    /* xWrite */
        redisVfsTruncate,                 /* xTruncate */
        redisVfsSync,                     /* xSync */
        redisVfsFileSize,                 /* xFileSize */
        redisVfsLock,                     /* xLock */
        redisVfsUnlock,                   /* xUnlock */
        redisVfsCheckReservedLock,        /* xCheckReservedLock */
        redisVfsFileControl,              /* xFileControl */
        redisVfsSectorSize,               /* xSectorSize */
        redisVfsDeviceCharacteristics     /* xDeviceCharacteristics */
    };

    RedisVfsFile *p = (RedisVfsFile*)pFile; /* Populate this structure */

    if (name==0)
        return SQLITE_IOERR;

    memset(p, 0, sizeof(RedisVfsFile));

    p->name = sqlite3_malloc(strlen(name)+1);
    if (!p->name) return SQLITE_NOMEM;
    strcpy(p->name,name);
    p->c = createClient(-1);

    p->base.pMethods = &redisVfs_methods;
    return SQLITE_OK;
}

static int redisVfsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync) {
    return SQLITE_OK;
}
static int redisVfsAccess(sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut) {
    *pResOut = 0; /* everything's always good */
    return SQLITE_OK;
}
static int redisVfsFullPathname(sqlite3_vfs *pVfs, const char *zPath, int nPathOut, char *zPathOut) {
    strncpy(zPathOut, zPath, nPathOut);
    zPathOut[nPathOut-1] = '\0';
    return SQLITE_OK;
}

static void *redisVfsDlOpen(sqlite3_vfs *pVfs, const char *zPath) {
    return 0;
}
static void redisVfsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg) {
    sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
    zErrMsg[nByte-1] = '\0';
}
static void (*redisVfsDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void) {
    return 0;
}
static void redisVfsDlClose(sqlite3_vfs *pVfs, void *pHandle) {
    return;
}
static int redisVfsRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte) {
    return SQLITE_OK;
}
static int redisVfsSleep(sqlite3_vfs *pVfs, int nMicro) {
    sleep(nMicro / 1000000);
    usleep(nMicro % 1000000);
    return nMicro;
}
static int redisVfsCurrentTime(sqlite3_vfs *pVfs, double *pTime) {
    time_t t = time(0);
    *pTime = t/86400.0 + 2440587.5; 
    return SQLITE_OK;
}

#define MAXPATHNAME 512
sqlite3_vfs *sqlite3_redis_vfs(void) {
    static sqlite3_vfs redisVfs = {
        1,                                /* iVersion */
        sizeof(RedisVfsFile),             /* szOsFile */
        MAXPATHNAME,                      /* mxPathname */
        0,                                /* pNext */
        "redis",                          /* name */
        0,                                /* pAppData */
        redisVfsOpen,                     /* xOpen */
        redisVfsDelete,                   /* xDelete */
        redisVfsAccess,                   /* xAccess */
        redisVfsFullPathname,             /* xFullPathname */
        redisVfsDlOpen,                   /* xDlOpen */
        redisVfsDlError,                  /* xDlError */
        redisVfsDlSym,                    /* xDlSym */
        redisVfsDlClose,                  /* xDlClose */
        redisVfsRandomness,               /* xRandomness */
        redisVfsSleep,                    /* xSleep */
        redisVfsCurrentTime,              /* xCurrentTime */
    };
    return &redisVfs;
}


