/*
 * Copyright (c) 2009-2013, Gregory Trubetskoy <grisha@apache.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "redis.h"
#include "sqlite3.h"

#include <assert.h>

#define REDIS_VTAB_MAGIC 12122012


/* BEGIN copy from t_zset.c */
typedef struct {
    robj *subject;
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    union {
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;

#define OPVAL_DIRTY_ROBJ 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    robj *ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

int zuiNext(zsetopsrc *op, zsetopval *val);
void zuiInitIterator(zsetopsrc *op);
void zuiClearIterator(zsetopsrc *op);
robj *zuiObjectFromValue(zsetopval *val);
/* END copy from t_zset.c */

typedef struct redis_vtab {
    sqlite3_vtab base;
    int magic;
    robj *name;
} redis_vtab;

typedef struct redis_cursor {
    sqlite3_vtab_cursor base;
    long pos;
    int eof;
    int opened_first;
    robj *robj;
    robj *name;
    union iter {
        struct {   /* REDIS_LIST */
            listTypeIterator *li;
            listTypeEntry *le;
        } list;
        struct {   /* REDIS_HASH */
            hashTypeIterator *hi;
        } hash;
        struct {   /* REDIS_SET and REDIS_ZSET */
            zsetopsrc *zi;
            zsetopval *zv;
        } zset;
    } iter;
} redis_cursor;

static int vt_destructor(sqlite3_vtab *pVtab)
{
    redis_vtab *p = (redis_vtab*)pVtab;
    p->magic = 0;
    decrRefCount(p->name);
    sqlite3_free(p);
    return 0;
}

static int vt_create(sqlite3 *db, void *aux, int argc, const char *const*argv,
                     sqlite3_vtab **s3_vtab, char **err ) {
    redis_vtab* vt;

    if ((vt = (redis_vtab*) sqlite3_malloc(sizeof(*vt))) == NULL)
        return SQLITE_NOMEM;

    vt->magic = REDIS_VTAB_MAGIC;
    vt->base.zErrMsg = 0; /* SQLite insists on this */
    vt->name = createObject(REDIS_STRING,sdsnew(argc > 3 ? argv[3] : argv[2]));

    /* declare the definition */
    if (sqlite3_declare_vtab(db,"create table vtable (key text, val text)") != SQLITE_OK) {
        vt_destructor((sqlite3_vtab*)vt);
        return SQLITE_ERROR;
    }

    /* Success. Set *result and return */
    *s3_vtab = &vt->base;

    return SQLITE_OK;
}

static int vt_connect(sqlite3 *db, void *aux, int argc, const char *const*argv,
                      sqlite3_vtab **s3_vtab, char **err) {
    return vt_create(db, aux, argc, argv, s3_vtab, err);
}

static int vt_disconnect(sqlite3_vtab *s3_vt) {
    return vt_destructor(s3_vt);
}

static int vt_destroy(sqlite3_vtab *s3_vt) {
    return vt_destructor(s3_vt);
}

static int vt_open(sqlite3_vtab *s3_vt, sqlite3_vtab_cursor **s3_cur) {
    redis_vtab *vt = (redis_vtab*)s3_vt;
    redis_cursor *cur;

    if (!(cur = (redis_cursor*)sqlite3_malloc(sizeof(redis_cursor)))) 
        return SQLITE_NOMEM;

    cur->name = vt->name;
    cur->pos = 0;

    *s3_cur = (sqlite3_vtab_cursor*)cur;
    return SQLITE_OK;
}

static int vt_close(sqlite3_vtab_cursor *s3_cur) {
    redis_cursor *cur = (redis_cursor*)s3_cur;

    if (cur->robj) {
        if (cur->robj->type == REDIS_LIST) {
            zfree(cur->iter.list.le);
        } else if (cur->robj->type == REDIS_ZSET || cur->robj->type == REDIS_SET) {
            zfree(cur->iter.zset.zi);
            zfree(cur->iter.zset.zv);
        } /* nothing to do for REDIS_HASH */
    }
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int vt_eof(sqlite3_vtab_cursor *cur) {
    return ((redis_cursor*)cur)->eof;
}

static int vt_next(sqlite3_vtab_cursor *s3_cur) {
    redis_cursor *cur = (redis_cursor*)s3_cur;

    if (cur->robj->type == REDIS_LIST) {
        if (!listTypeNext(cur->iter.list.li, cur->iter.list.le)) {
            cur->eof = 1;
            listTypeReleaseIterator(cur->iter.list.li);
            return SQLITE_OK;
        }
    } else if (cur->robj->type == REDIS_HASH) {
        if (hashTypeNext(cur->iter.hash.hi) == REDIS_ERR) {
            cur->eof = 1;
            hashTypeReleaseIterator(cur->iter.hash.hi);
            return SQLITE_OK;
        }
    } else if (cur->robj->type == REDIS_ZSET || cur->robj->type == REDIS_SET) {
        if (!zuiNext(cur->iter.zset.zi, cur->iter.zset.zv)) {
            cur->eof = 1;
            zuiClearIterator(cur->iter.zset.zi);
            return SQLITE_OK;
        }
    } else if (cur->robj->type == REDIS_STRING)
        cur->eof = cur->pos;

    cur->pos += 1;
    return SQLITE_OK;
}

static int vt_column(sqlite3_vtab_cursor *s3_cur, sqlite3_context *ctx, int i)
{
    redis_cursor *cur = (redis_cursor *) s3_cur;
    robj *o;

    /* If the object is encoded as a ziplist, we get a copy, which we
     * must decref right here (which causes the memory to be freed
     * immediately). For other types of encoding, we should get an
     * actual memory pointer, zero-copy. We use a trick here - examine
     * the refcount. If it is greater than 1, then we have a zero-copy
     * object, and it is safe to pass it to SQLite as SQLITE_STATIC.
     * Otherwise (refcount is 1), means we have a freshly created copy
     * which we must pass as SQLITE_TRANSIENT (causing SQLite to make
     * a second copy!) then free. Curiously, we can only have either
     * zero-copy or double-copy behaviour. */

    if (cur->robj->type == REDIS_STRING) {
        if (i == 0)
            sqlite3_result_int(ctx, cur->pos);
        else {
            o = cur->robj;
            if (o->encoding == REDIS_ENCODING_RAW)
                sqlite3_result_text(ctx,o->ptr,sdslen(o->ptr),SQLITE_STATIC);
            else
                sqlite3_result_int64(ctx,(long)o->ptr);
        }
    } else if (cur->robj->type == REDIS_LIST) {
        if (i == 0)
            sqlite3_result_int(ctx, cur->pos);
        else {
            o = listTypeGet(cur->iter.list.le);
            if (o->encoding == REDIS_ENCODING_RAW)
                sqlite3_result_text(ctx,o->ptr,sdslen(o->ptr),
                                    cur->robj->refcount > 1 ?
                                    SQLITE_STATIC : SQLITE_TRANSIENT);
            else
                sqlite3_result_int64(ctx,(long)o->ptr);
            decrRefCount(o);
        }
    } else if (cur->robj->type == REDIS_HASH) {
        if (i == 0)
            o = hashTypeCurrentObject(cur->iter.hash.hi, REDIS_HASH_KEY);
        else
            o = hashTypeCurrentObject(cur->iter.hash.hi, REDIS_HASH_VALUE);
        if (o->encoding == REDIS_ENCODING_RAW)
            sqlite3_result_text(ctx,o->ptr,sdslen(o->ptr),
                                cur->robj->refcount > 1 ?
                                SQLITE_STATIC : SQLITE_TRANSIENT);
        else
            sqlite3_result_int64(ctx,(long)o->ptr);
        decrRefCount(o);

    } else if (cur->robj->type == REDIS_ZSET || cur->robj->type == REDIS_SET) {
        if (i == 0)
            sqlite3_result_double(ctx, cur->iter.zset.zv->score);
        else {
            o = zuiObjectFromValue(cur->iter.zset.zv);
            if (o->encoding == REDIS_ENCODING_RAW)
                sqlite3_result_text(ctx,o->ptr,sdslen(o->ptr),
                                    cur->robj->refcount > 1 ?
                                    SQLITE_STATIC : SQLITE_TRANSIENT);
            else
                sqlite3_result_int64(ctx,(long)o->ptr);
            /* no need for decrRefCount(o), zuiNext will do that */
        }
    }
    return SQLITE_OK;
}

static int vt_rowid(sqlite3_vtab_cursor *s3_cur, sqlite_int64 *p_rowid) {
    /* Just use the current row count as the rowid. */
    *p_rowid = ((redis_cursor*)s3_cur)->pos;
    return SQLITE_OK;
}

static int vt_filter( sqlite3_vtab_cursor *s3_cur,
                      int idxNum, const char *idxStr,
                      int argc, sqlite3_value **argv ) {
    redis_cursor *cur = (redis_cursor*)s3_cur;

    if ((cur->robj = lookupKeyRead(&server.db[0], cur->name)) == NULL) {
        /* non-existent redis object will simply result in an empty set */
        cur->eof = 1;
        return SQLITE_OK;
    }
    cur->eof = 0;

    if (cur->robj->type == REDIS_LIST) {
        cur->iter.list.le = zmalloc(sizeof(listTypeEntry));
        cur->iter.list.li = listTypeInitIterator(cur->robj,0,REDIS_TAIL);
    } else if (cur->robj->type == REDIS_ZSET || cur->robj->type == REDIS_SET) {
        cur->iter.zset.zi = zcalloc(sizeof(zsetopsrc));
        cur->iter.zset.zv = zcalloc(sizeof(zsetopval));
        cur->iter.zset.zi->subject = cur->robj;
        cur->iter.zset.zi->type = cur->robj->type;
        cur->iter.zset.zi->encoding = cur->robj->encoding;
        zuiInitIterator(cur->iter.zset.zi);
    } else if (cur->robj->type == REDIS_HASH)
        cur->iter.hash.hi = hashTypeInitIterator(cur->robj);
    /* nothing to do for other types */

    /* Move cursor to first row. */
    return vt_next(s3_cur);
}

static int vt_best_index(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo) {
    return SQLITE_OK;
}

static sqlite3_module redis_module =
{
    0,              /* iVersion */
    vt_create,      /* xCreate       - create a vtable */
    vt_connect,     /* xConnect      - associate a vtable with a connection */
    vt_best_index,  /* xBestIndex    - best index */
    vt_disconnect,  /* xDisconnect   - disassociate a vtable with a connection */
    vt_destroy,     /* xDestroy      - destroy a vtable */
    vt_open,        /* xOpen         - open a cursor */
    vt_close,       /* xClose        - close a cursor */
    vt_filter,      /* xFilter       - configure scan constraints */
    vt_next,        /* xNext         - advance a cursor */
    vt_eof,         /* xEof          - inidicate end of result set*/
    vt_column,      /* xColumn       - read data */
    vt_rowid,       /* xRowid        - read data */
    NULL,           /* xUpdate       - write data */
    NULL,           /* xBegin        - begin transaction */
    NULL,           /* xSync         - sync transaction */
    NULL,           /* xCommit       - commit transaction */
    NULL,           /* xRollback     - rollback transaction */
    NULL,           /* xFindFunction - function overloading */
};


/************************************************/

char *redisProtocolToSQLType(sqlite3_context *ctx, sds *sql_reply, char *reply);
char *redisProtocolToSQLType_Int(sqlite3_context *ctx, sds *sql_reply, char *reply);
char *redisProtocolToSQLType_Bulk(sqlite3_context *ctx, sds *sql_reply, char *reply);
char *redisProtocolToSQLType_MultiBulk(sqlite3_context *ctx, sds *sql_reply, char *reply);

static void redis_func(sqlite3_context *ctx, int argc, sqlite3_value **sql_argv) {
    int j;
    robj **argv;
    struct redisCommand *cmd;
    sds reply, sql_reply;
    redisClient *c = ((redisClient *)sqlite3_user_data(ctx))->sql_client;

    /* require at least one argument */
    if (argc == 0) {
        sqlite3_result_error(ctx, "Please specify at least one argument for redis()", -1);
        return;
    }

    /* build argv */
    argv = zmalloc(sizeof(robj*)*argc);
    for (j = 0; j < argc; j++) {
        int len = sqlite3_value_bytes(sql_argv[j]);
        argv[j] = createStringObject((char *)sqlite3_value_text(sql_argv[j]), len);
    }

    /* lock before running redis commands */
    pthread_mutex_lock(c->lock);

    /* setup fake client for command execution */
    c->argc = argc;
    c->argv = argv;

    /* command lookup */
    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd || ((cmd->arity > 0 && cmd->arity != argc) ||
                   (argc < -cmd->arity)))
    {
        if (cmd)
            sqlite3_result_error(ctx, "Wrong number of args calling Redis command from SQL", -1);
        else
            sqlite3_result_error(ctx, "Unknown Redis command called from SQL", -1);
        goto cleanup;
    }

    /* same rule as Lua + no db switching + sql itself*/
    // THREDIS TODO perhaps Lua *should* be allowed?
    if (cmd->flags & REDIS_CMD_NOSCRIPT ||
        cmd->proc == selectCommand || cmd->proc == sqlCommand) {
        sqlite3_result_error(ctx, "This Redis command is not allowed from SQL", -1);
        goto cleanup;
    }

    /* write commands are sometimes forbidden THREDIS TODO: this needs refinement */
    if (cmd->flags & REDIS_CMD_WRITE) {
        if (server.stop_writes_on_bgsave_err &&
            server.saveparamslen > 0 &&
            server.lastbgsave_status == REDIS_ERR)
        {
            sqlite3_result_error(ctx, shared.bgsaveerr->ptr, -1);
            goto cleanup;
        }
    }

    /* are we reaching memory limits */
    if (server.maxmemory &&
        (cmd->flags & REDIS_CMD_DENYOOM))
    {
        if (freeMemoryIfNeeded() == REDIS_ERR) {
            sqlite3_result_error(ctx, shared.oomerr->ptr, -1);
            goto cleanup;
        }
    }

    /* Run the command */
    c->cmd = cmd;
    call(c,REDIS_CALL_SLOWLOG | REDIS_CALL_STATS);

    // THREDIS TODO convert result to a suitable type, e.g. json
    reply = sdsempty();
    if (c->bufpos) {
        reply = sdscatlen(reply,c->buf,c->bufpos);
        c->bufpos = 0;
    }

    sql_reply = sdsempty();
    if (redisProtocolToSQLType(ctx, &sql_reply, reply) != NULL)
        sqlite3_result_text(ctx, sql_reply, sdslen(sql_reply), SQLITE_TRANSIENT);
    sdsfree(sql_reply);

  cleanup:
    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);

    pthread_mutex_unlock(c->lock);
}

/* initialize the SQLite in-memory database */
void sqlInit(void) {
    if (sqlite3_open_v2("file::memory:?cache=shared", &server.sql_db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_SHAREDCACHE,
                        NULL)) {
        redisLog(REDIS_WARNING, "Could not initialize SQLite database, exiting.");
        exit(1);
    }
}

/* initialize the per-client SQLite in-memory database connection */
void sqlClientInit(redisClient *c) {
    if (sqlite3_open_v2("file::memory:?cache=shared", &c->sql_db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_SHAREDCACHE,
                        NULL)) {
        redisLog(REDIS_WARNING, "Could not create SQLite database connection, exiting.");
        exit(1);
    }
    c->sql_client = createClient(-1);
    c->sql_client->flags |= REDIS_SQLITE_CLIENT;

    sqlite3_create_function(c->sql_db, "redis", -1, SQLITE_ANY, (void*)c, redis_func, NULL, NULL);
    sqlite3_create_module(c->sql_db, "redis", &redis_module, NULL);
}

/* close per-client SQLite in-memory database connection */
void sqlClientClose(redisClient *c) {
    if (sqlite3_close_v2(c->sql_db) != SQLITE_OK)
        redisLog(REDIS_WARNING, "Call to sqlite3_close_v2() failed.");
    zfree(c->sql_client);
}

/* These are necessary to scan Vdbe ops in the SQLite statement. Vdbe
 * interface is hidden and so we have to resort to brutal measures
 * like this to peek inside it  */
#define OP_VOpen 135
typedef struct fake_vtable {
    sqlite3 *db;
    char *pMod;
    sqlite3_vtab *pVtab;
    int nRef;
    uint8_t bConstraint;
    int iSavepoint;
    struct fake_vtable *pNext;
} fake_vtable;
typedef struct fake_op {
    uint8_t opcode;
    signed char p4type;
    uint8_t opflags;
    uint8_t p5;
    int p1;
    int p2;
    int p3;
    fake_vtable *pVtab;
} fake_op;
typedef struct fake_vdbe {
  sqlite3 *db;
  fake_op *aOp;
  char *aMem;
  char **apArg;
  char *aColName;
  char *pResultSet;
  int nMem;
  int nOp;
} fake_vdbe;

/* scan the statement for any vtable open instructions, check whether
 * its one of ours (by looking for a magic number). this is used to
 * lock the affected redis keys */
static robj **scan_stmt_for_redis_vtabs(sqlite3_stmt *stmt, int *n_keys) {
    int i;
    robj **keys = NULL;
    listNode *ln;
    listIter *li;
    fake_vdbe *v = (fake_vdbe *)stmt;
    list *list = listCreate();
    for (i=0; i<v->nOp; i++) {
        if (v->aOp[i].opcode == OP_VOpen) {
            redis_vtab *vt = (redis_vtab *)v->aOp[i].pVtab->pVtab;
            if (vt->magic == REDIS_VTAB_MAGIC)
                listAddNodeTail(list, vt->name);
        }
    }
    *n_keys = listLength(list);
    if (*n_keys == 0)
        return NULL;
    keys = zmalloc(sizeof(robj *) * *n_keys);
    li = listGetIterator(list,AL_START_HEAD);
    i = 0;
    while ((ln = listNext(li)))
        keys[i++] = (robj*)ln->value;
    listReleaseIterator(li);
    listRelease(list);
    return keys;
}

/******************************************************
 *  The following is taken from:
 * http://www.sqlite.org/unlock_notify.html - we prefer that our
 * functions blocked when lock is not available.
 */
/*
** A pointer to an instance of this structure is passed as the user-context
** pointer when registering for an unlock-notify callback.
*/
typedef struct UnlockNotification UnlockNotification;
struct UnlockNotification {
    int fired;                         /* True after unlock event has occurred */
    pthread_cond_t cond;               /* Condition variable to wait on */
    pthread_mutex_t mutex;             /* Mutex to protect structure */
};
/*
** This function is an unlock-notify callback registered with SQLite.
*/
static void unlock_notify_cb(void **apArg, int nArg){
    int i;
    for(i=0; i<nArg; i++){
        UnlockNotification *p = (UnlockNotification *)apArg[i];
        pthread_mutex_lock(&p->mutex);
        p->fired = 1;
        pthread_cond_signal(&p->cond);
        pthread_mutex_unlock(&p->mutex);
    }
}
/*
** This function assumes that an SQLite API call (either sqlite3_prepare_v2()
** or sqlite3_step()) has just returned SQLITE_LOCKED. The argument is the
** associated database connection.
**
** This function calls sqlite3_unlock_notify() to register for an
** unlock-notify callback, then blocks until that callback is delivered
** and returns SQLITE_OK. The caller should then retry the failed operation.
**
** Or, if sqlite3_unlock_notify() indicates that to block would deadlock
** the system, then this function returns SQLITE_LOCKED immediately. In
** this case the caller should not retry the operation and should roll
** back the current transaction (if any).
*/
static int wait_for_unlock_notify(sqlite3 *db){
    int rc;
    UnlockNotification un;

    /* Initialize the UnlockNotification structure. */
    un.fired = 0;
    pthread_mutex_init(&un.mutex, 0);
    pthread_cond_init(&un.cond, 0);

    /* Register for an unlock-notify callback. */
    rc = sqlite3_unlock_notify(db, unlock_notify_cb, (void *)&un);
    assert( rc==SQLITE_LOCKED || rc==SQLITE_OK );

    /* The call to sqlite3_unlock_notify() always returns either SQLITE_LOCKED
    ** or SQLITE_OK.
    **
    ** If SQLITE_LOCKED was returned, then the system is deadlocked. In this
    ** case this function needs to return SQLITE_LOCKED to the caller so
    ** that the current transaction can be rolled back. Otherwise, block
    ** until the unlock-notify callback is invoked, then return SQLITE_OK.
    */
    if( rc==SQLITE_OK ){
        pthread_mutex_lock(&un.mutex);
        if( !un.fired ){
            pthread_cond_wait(&un.cond, &un.mutex);
        }
        pthread_mutex_unlock(&un.mutex);
    }

    /* Destroy the mutex and condition variables. */
    pthread_cond_destroy(&un.cond);
    pthread_mutex_destroy(&un.mutex);

    return rc;
}
/*
** This function is a wrapper around the SQLite function sqlite3_step().
** It functions in the same way as step(), except that if a required
** shared-cache lock cannot be obtained, this function may block waiting for
** the lock to become available. In this scenario the normal API step()
** function always returns SQLITE_LOCKED.
**
** If this function returns SQLITE_LOCKED, the caller should rollback
** the current transaction (if any) and try again later. Otherwise, the
** system may become deadlocked.
*/
int sqlite3_blocking_step(sqlite3_stmt *pStmt){
    int rc;
    while( SQLITE_LOCKED==(rc = sqlite3_step(pStmt)) ){
        rc = wait_for_unlock_notify(sqlite3_db_handle(pStmt));
        if( rc!=SQLITE_OK ) break;
        sqlite3_reset(pStmt);
    }
    return rc;
}
/*
** This function is a wrapper around the SQLite function sqlite3_prepare_v2().
** It functions in the same way as prepare_v2(), except that if a required
** shared-cache lock cannot be obtained, this function may block waiting for
** the lock to become available. In this scenario the normal API prepare_v2()
** function always returns SQLITE_LOCKED.
**
** If this function returns SQLITE_LOCKED, the caller should rollback
** the current transaction (if any) and try again later. Otherwise, the
** system may become deadlocked.
*/
int sqlite3_blocking_prepare_v2(
    sqlite3 *db,              /* Database handle. */
    const char *zSql,         /* UTF-8 encoded SQL statement. */
    int nSql,                 /* Length of zSql in bytes. */
    sqlite3_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
    const char **pz           /* OUT: End of parsed string */
    ){
    int rc;
    while( SQLITE_LOCKED==(rc = sqlite3_prepare_v2(db, zSql, nSql, ppStmt, pz)) ){
        rc = wait_for_unlock_notify(db);
        if( rc!=SQLITE_OK ) break;
    }
    return rc;
}
/******************************************************
 * End of stuff from http://www.sqlite.org/unlock_notify.html
 */

void sqlGenericCommand(redisClient *c, int prepare_only) {
    int rc = SQLITE_OK;
    const char *leftover;
    const char *sql = c->argv[1]->ptr;
    sqlite3_stmt *stmt = NULL;
    int rows_sent = 0;
    int *replylen = NULL;
    robj **keys = NULL;
    int n_keys = 0;
    int retries = 0;

    /* this is necessary to get enlish errors, see http://www.sqlite.org/c3ref/errcode.html */
    sqlite3_mutex_enter(sqlite3_db_mutex(c->sql_db));

    while ((rc==SQLITE_OK || (rc==SQLITE_SCHEMA && (++retries)<2)) && sql[0]) {
        int n_cols, i;

        if ((rc = sqlite3_blocking_prepare_v2(c->sql_db, sql, -1, &stmt, &leftover)) != SQLITE_OK)
            continue;   /* possibly SQLITE_SCHEMA, try again, else will exit loop */

        if (!stmt) {    /* this happens for a comment or white-space */
            sql = leftover;
            continue;
        }

        n_cols = sqlite3_column_count(stmt);

        if (n_cols > 0) { /* write column names */
            replylen = addDeferredMultiBulkLength(c);
            addReplyMultiBulkLen(c, n_cols);
            for (i=0; i<n_cols; i++) {
                char *type;
                addReplyMultiBulkLen(c,2);
                addReplyBulkCString(c,(char *)sqlite3_column_name(stmt,i));
                if ((type = (char *)sqlite3_column_decltype(stmt,i)) != NULL)
                    addReplyBulkCString(c,type);
                else {
                    switch (sqlite3_column_type(stmt, i)) {
                    case SQLITE_INTEGER:
                        addReplyBulkCString(c,"int");
                        break;
                    case SQLITE_FLOAT:
                        addReplyBulkCString(c,"real");
                        break;
                    case SQLITE_BLOB:
                        addReplyBulkCString(c,"blob");
                        break;
                    default:
                        addReplyBulkCString(c,"text");
                    }
                }
            }
            rows_sent++;
        }

        /* if this is a sqlprepare, there is nothing more to do. this
         * is a hack, the real solution should cache the prepared
         * statement locally in the connection and bind parameters to it
         * without preparing at subsequent SQL commands with the same
         * statment. THREDIS TODO */
        if (prepare_only) break;

        /* bind parameters, if any */
        if (c->argc > 2)
            for (i=2; i<c->argc; i++) {

                /* We need a way to pass a NULL here. We could use
                 * Redis's bulk $-1, but that would be too radical of
                 * a change to the protocol (currently only the server
                 * can send a $-1, if client sends a $-1 it's an
                 * error). We use ":NULL" to mean NULL, and for
                 * anything else that begins with a ":" we drop the
                 * first ":" */

                if (sdslen(c->argv[i]->ptr) == 5 && !memcmp(c->argv[i]->ptr, ":NULL", 5))
                    sqlite3_bind_null(stmt, i-1);
                else if (sdslen(c->argv[i]->ptr) > 1 && ((char*)c->argv[i]->ptr)[0] == ':')
                    sqlite3_bind_text(stmt, i-1, ((char*)c->argv[i]->ptr)+1, sdslen(c->argv[1]->ptr)-1, SQLITE_STATIC);
                else {
                    c->argv[i] = tryObjectEncoding(c->argv[i]);
                    if (c->argv[i]->encoding == REDIS_ENCODING_RAW)
                        sqlite3_bind_text(stmt, i-1, c->argv[i]->ptr, sdslen(c->argv[i]->ptr), SQLITE_STATIC);
                    else
                        sqlite3_bind_int64(stmt, i-1, (long)c->argv[i]->ptr);
                }
            }
        /* figure out which keys to lock by using this crazy hack */
        keys = scan_stmt_for_redis_vtabs(stmt, &n_keys);
        if (keys)
            lockKeys(c, keys, n_keys);

        while ((rc = sqlite3_blocking_step(stmt)) == SQLITE_ROW) {
            sqlite3_mutex_leave(sqlite3_db_mutex(c->sql_db));

            addReplyMultiBulkLen(c,n_cols);
            for (i=0; i<n_cols; i++) {
                if (sqlite3_column_type(stmt,i) == SQLITE_INTEGER)
                    addReplyLongLong(c,sqlite3_column_int64(stmt,i));
                else {
                    char *txt = (char *)sqlite3_column_text(stmt,i);
                    if (!txt)
                        addReply(c,shared.nullbulk);
                    else
                        addReplyBulkCBuffer(c,(void*)txt,sqlite3_column_bytes(stmt,i));
                }
            }
            rows_sent++;

            sqlite3_mutex_enter(sqlite3_db_mutex(c->sql_db));
        }
    }

    if (stmt) sqlite3_finalize(stmt);

    if (keys) {
        unlockKeys(c, keys, n_keys);
        zfree(keys);
    }

    if (rc != SQLITE_OK && rc != SQLITE_DONE)
        addReplyErrorFormat(c,"SQL error: %s\n",sqlite3_errmsg(c->sql_db));
    else if (rows_sent > 0)
        setDeferredMultiBulkLength(c,replylen,rows_sent);
    else
        addReply(c, shared.ok);

    pthread_mutex_lock(server.lock);
    server.dirty += sqlite3_changes(c->sql_db);
    pthread_mutex_unlock(server.lock);

    sqlite3_mutex_leave(sqlite3_db_mutex(c->sql_db));
}

void sqlCommand(redisClient *c) {
    sqlGenericCommand(c, 0);
}

void sqlprepareCommand(redisClient *c) {
    sqlGenericCommand(c, 1);
}

int loadOrSaveDb(sqlite3 *inmemory, const char *filename, int is_save) {
    int rc;
    sqlite3 *file;
    sqlite3_backup *backup;
    sqlite3 *to;
    sqlite3 *from;

    /* Open the database file identified by filename. Exit early if
     * this fails for any reason. */

    rc = sqlite3_open(filename, &file);

    if (rc==SQLITE_OK) {

        from = is_save ? inmemory : file;
        to   = is_save ? file : inmemory;

        backup = sqlite3_backup_init(to, "main", from, "main");
        if (backup) {
            sqlite3_backup_step(backup, -1);
            sqlite3_backup_finish(backup);
        }
        rc = sqlite3_errcode(to);
    }

    sqlite3_close(file);

    if (rc == SQLITE_OK)
        redisLog(REDIS_NOTICE,"SQL DB saved on disk");
    else
        redisLog(REDIS_WARNING, "Error saving SQL DB on disk: %s", strerror(errno));

    /* SQLITE_OK and REDIS_OK are the same value: 0 */
    return rc;
}

void sqlsaveCommand(redisClient *c) {

    if (loadOrSaveDb(server.sql_db, server.sql_filename, 1) != SQLITE_OK)
        addReplyError(c,"Error while saving SQL data.");
    else
        addReply(c, shared.ok);
}

void sqlloadCommand(redisClient *c) {

    if (loadOrSaveDb(server.sql_db, server.sql_filename, 0) != SQLITE_OK)
        // THREDIS TODO - should we panic?
        addReplyError(c,"Error while loading SQL data.");
    else
        addReply(c, shared.ok);
}

char *redisProtocolToSQLType(sqlite3_context *ctx, sds *sql_reply, char *reply) {
    char *p = reply;

    switch(*p) {
    case '+':
        sqlite3_result_null(ctx); /* status is just a NULL */
        return NULL;
    case '-':
        p = strchr(reply+1,'\r');
        sqlite3_result_error(ctx,reply+1,p-reply-1);
        return NULL;
    case ':':
        p = redisProtocolToSQLType_Int(ctx,sql_reply,reply);
        break;
    case '$':
        p = redisProtocolToSQLType_Bulk(ctx,sql_reply,reply);
        break;
    case '*':
        p = redisProtocolToSQLType_MultiBulk(ctx,sql_reply,reply);
        break;
    }
    return p;
}

char *redisProtocolToSQLType_Int(sqlite3_context *ctx, sds *sql_reply, char *reply) {
    char *p = strchr(reply+1,'\r');

    *sql_reply = sdscatlen(*sql_reply,reply+1,p-reply-1);
    return p+2;
}

char *redisProtocolToSQLType_Bulk(sqlite3_context *ctx, sds *sql_reply, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long bulklen;

    string2ll(reply+1,p-reply-1,&bulklen);
    if (bulklen == -1) {
        /* NULL - do nothing */
        return p+2;
    } else {
        *sql_reply = sdscatlen(*sql_reply,p+2,bulklen);
        return p+2+bulklen+2;
    }
}

char *redisProtocolToSQLType_MultiBulk(sqlite3_context *ctx, sds *sql_reply, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    if (mbulklen == -1) {
        /* NULL - do nothing */
        return p;
    }
    // THREDIS TODO This could be Json-encoded for realz
    if (mbulklen>1)
        *sql_reply = sdscatlen(*sql_reply,"[",1);
    for (j = 0; j < mbulklen; j++) {
        p = redisProtocolToSQLType(ctx,sql_reply,p);
        if (!p) return NULL; /* error or status */
        if (j<mbulklen-1)
            *sql_reply = sdscat(*sql_reply,",");
    }
    if (mbulklen>1)
        *sql_reply = sdscat(*sql_reply,"]");
    return p;
}

