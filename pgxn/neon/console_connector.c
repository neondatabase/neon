#include "postgres.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "fmgr.h"
#include <curl/curl.h>

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static CURL *CurlHandle = NULL;

typedef enum
{
    Op_Set,
    Op_Delete,
} OpType;

typedef struct
{
    char name[NAMEDATALEN];
    char owner[NAMEDATALEN];
    char old_name[NAMEDATALEN];
    OpType type;
} DbEntry;

typedef struct
{
    char name[NAMEDATALEN];
    char old_name[NAMEDATALEN];
    const char *password;
    OpType type;
} RoleEntry;

typedef struct DdlHashTable
{
    struct DdlHashTable *prev_table;
    HTAB *role_table;
    HTAB *db_table;
} DdlHashTable;

static DdlHashTable RootTable;
static DdlHashTable *CurrentDdlTable = &RootTable;

static void SendDeltasToConsole()
{
    
}

static void PushTable()
{
    DdlHashTable *new_table = MemoryContextAlloc(CurTransactionContext, sizeof(DdlHashTable));
    new_table->prev_table = CurrentDdlTable;
    CurrentDdlTable = new_table;

    HASHCTL db_ctl = {};
    db_ctl.keysize = NAMEDATALEN;
    db_ctl.entrysize = sizeof(DbEntry);
    db_ctl.hcxt = CurTransactionContext;
    new_table->db_table = hash_create(
        "Dbs Created",
        4,
        &db_ctl,
        HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

    HASHCTL role_ctl = {};
    role_ctl.keysize = NAMEDATALEN;
    role_ctl.entrysize = sizeof(RoleEntry);
    role_ctl.hcxt = CurTransactionContext;
    new_table->role_table = hash_create(
        "Roles Created",
        4,
        &role_ctl,
        HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}

static void MergeTable()
{
    DdlHashTable *old_table = CurrentDdlTable;
    CurrentDdlTable = old_table->prev_table;

    {
        DbEntry *entry;
        HASH_SEQ_STATUS status;
        hash_seq_init(&status, old_table->db_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            DbEntry *to_write = hash_search(
                CurrentDdlTable->db_table,
                entry->name,
                HASH_ENTER,
                NULL);

            to_write->type = entry->type;
            if(entry->owner[0] != '\0')
                strncpy(to_write->owner, entry->owner, NAMEDATALEN);
            strncpy(to_write->old_name, entry->old_name, NAMEDATALEN);
            if(entry->old_name[0] != '\0')
            {
                bool found_old = false;
                DbEntry *old = hash_search(
                    CurrentDdlTable->db_table,
                    entry->old_name,
                    HASH_FIND,
                    &found_old);
                if(found_old)
                {
                    if(old->old_name[0] != '\0')
                        strncpy(to_write->old_name, old->old_name, NAMEDATALEN);
                    else
                        strncpy(to_write->old_name, entry->old_name, NAMEDATALEN);
                    hash_search(
                        CurrentDdlTable->db_table,
                        entry->old_name,
                        HASH_REMOVE,
                        NULL);
                }
            }
        }
        hash_destroy(old_table->db_table);
    }

    {
        RoleEntry *entry;
        HASH_SEQ_STATUS status;
        hash_seq_init(&status, old_table->role_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            RoleEntry *to_write = hash_search(
                CurrentDdlTable->role_table,
                entry->name,
                HASH_ENTER,
                NULL);
            to_write->type = entry->type;
            if(entry->password)
                to_write->password = entry->password;
            strncpy(to_write->old_name, entry->old_name, NAMEDATALEN);
            if(entry->old_name[0] != '\0')
            {
                bool found_old = false;
                RoleEntry *old = hash_search(
                    CurrentDdlTable->role_table,
                    entry->name,
                    HASH_FIND,
                    &found_old);
                if(found_old)
                {
                    if(old->old_name[0] != '\0')
                        strncpy(to_write->old_name, old->old_name, NAMEDATALEN);
                    else
                        strncpy(to_write->old_name, entry->old_name, NAMEDATALEN);
                    hash_search(CurrentDdlTable->role_table,
                                entry->old_name,
                                HASH_REMOVE,
                                NULL);
                }
            }
        }
        hash_destroy(old_table->role_table);
    }
}

static void PopTable()
{
    // Current table gets freed because it is allocated in aborted subtransaction's
    // memory context
    CurrentDdlTable = CurrentDdlTable->prev_table;
}

static void NeonSubXactCallback(
    SubXactEvent event,
    SubTransactionId mySubid,
    SubTransactionId parentSubid,
    void *arg)
{
    switch(event)
    {
    case SUBXACT_EVENT_START_SUB:
        return PushTable();
    case SUBXACT_EVENT_COMMIT_SUB:
        return MergeTable();
    case SUBXACT_EVENT_ABORT_SUB:
        return PopTable();
    default:
        return;
    }
}

static void NeonXactCallback(XactEvent event, void *arg)
{
    if(event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT)
    {
        SendDeltasToConsole();
    }
}

static void HandleCreateDb(CreatedbStmt *stmt)
{
    DefElem *downer = NULL;
    ListCell *option;
    foreach(option, stmt->options)
    {
        DefElem *defel = lfirst(option);
        if(strcmp(defel->defname, "owner") == 0)
            downer = defel;
    }
    bool found = false;
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        stmt->dbname,
        HASH_ENTER,
        &found);

    entry->type = Op_Set;
    if(downer)
        strncpy(entry->owner, defGetString(downer), NAMEDATALEN);
    else
        strncpy(entry->owner, GetUserNameFromId(GetUserId(), false), NAMEDATALEN);
}

static void HandleAlterOwner(AlterOwnerStmt *stmt)
{
    if(stmt->objectType != OBJECT_DATABASE)
        return;
    const char *name = strVal(stmt->object);
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        name,
        HASH_ENTER,
        NULL);

    strncpy(entry->owner, get_rolespec_name(stmt->newowner), NAMEDATALEN);
    entry->type = Op_Set;
}

static void HandleDbRename(RenameStmt *stmt)
{
    Assert(stmt->renameType == OBJECT_DATABASE);
    const char *name = strVal(stmt->object);
    bool found = false;
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        name,
        HASH_FIND,
        &found);
    DbEntry *entry_for_new_name = hash_search(
        CurrentDdlTable->db_table,
        stmt->newname,
        HASH_ENTER,
        NULL);
    entry_for_new_name->type = Op_Set;
    if(found)
    {
        if(entry->old_name[0] != '\0')
            strncpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
        else
            strncpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        strncpy(entry_for_new_name->owner, entry->owner, NAMEDATALEN);
        hash_search(
            CurrentDdlTable->db_table,
            name,
            HASH_REMOVE,
            NULL);
    }
    else
    {
        strncpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        entry_for_new_name->owner[0] = '\0';
    }
}

static void HandleDropDb(DropdbStmt *stmt)
{
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        stmt->dbname,
        HASH_ENTER,
        NULL);
    entry->type = Op_Delete;
}

static void HandleCreateRole(CreateRoleStmt *stmt)
{
    RoleEntry *entry = hash_search(
        CurrentDdlTable->role_table,
        stmt->role,
        HASH_ENTER,
        NULL);
    DefElem *dpass = NULL;
    ListCell *option;
    foreach(option, stmt->options)
    {
        DefElem *defel = lfirst(option);
        if(strcmp(defel->defname, "password") == 0)
            dpass = defel;
    }
    if(dpass)
        entry->password = MemoryContextStrdup(CurTransactionContext, strVal(dpass->arg));
    else
        entry->password = NULL;
    entry->type = Op_Set;
}

static void HandleAlterRole(AlterRoleStmt *stmt)
{
    DefElem *dpass = NULL;
    ListCell *option;
    foreach(option, stmt->options)
    {
        DefElem *defel = lfirst(option);
        if(strcmp(defel->defname, "password") == 0)
            dpass = defel;
    }
    // We're not updating the password
    if(!dpass)
        return;
    RoleEntry *entry = hash_search(
        CurrentDdlTable->role_table,
        stmt->role,
        HASH_ENTER,
        NULL);
    entry->password = strVal(dpass->arg);
    entry->type = Op_Set;
}

static void HandleRoleRename(RenameStmt *stmt)
{
    Assert(stmt->renameType == OBJECT_ROLE);
    const char *name = strVal(stmt->object);
    bool found = false;
    RoleEntry *entry = hash_search(
        CurrentDdlTable->role_table,
        name,
        HASH_FIND,
        &found);

    RoleEntry *entry_for_new_name = hash_search(
        CurrentDdlTable->role_table,
        stmt->newname,
        HASH_ENTER,
        NULL);
    entry_for_new_name->type = Op_Set;
    if(found)
    {
        if(entry->old_name[0] != '\0')
            strncpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
        else
            strncpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        entry_for_new_name->password = entry->password;
        hash_search(
            CurrentDdlTable->db_table,
            name,
            HASH_REMOVE,
            NULL);
    }
    else
    {
        strncpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        entry_for_new_name->password = NULL;
    }
}

static void HandleDropRole(DropRoleStmt *stmt)
{
    ListCell *item;
    foreach(item, stmt->roles)
    {
        RoleSpec *spec = lfirst(item);
        DbEntry *entry = hash_search(
            CurrentDdlTable->role_table,
            spec->rolename,
            HASH_ENTER,
            NULL);
        entry->type = Op_Delete;
    }
}

static void HandleRename(RenameStmt *stmt)
{
    if(stmt->renameType == OBJECT_DATABASE)
        return HandleDbRename(stmt);
    else if(stmt->renameType == OBJECT_ROLE)
        return HandleRoleRename(stmt);
}

static void NeonProcessUtility(
    PlannedStmt *pstmt,
    const char *queryString,
    bool readOnlyTree,
    ProcessUtilityContext context,
    ParamListInfo params,
    QueryEnvironment *queryEnv,
    DestReceiver *dest,
    QueryCompletion *qc)
{
    Node *parseTree = pstmt->utilityStmt;
    switch(nodeTag(parseTree))
    {
    case T_CreatedbStmt:
        HandleCreateDb(castNode(CreatedbStmt, parseTree));
        break;
    case T_AlterOwnerStmt:
        HandleAlterOwner(castNode(AlterOwnerStmt, parseTree));
        break;
    case T_RenameStmt:
        HandleRename(castNode(RenameStmt, parseTree));
        break;
    case T_DropdbStmt:
        HandleDropDb(castNode(DropdbStmt, parseTree));
        break;
    case T_CreateRoleStmt:
        HandleCreateRole(castNode(CreateRoleStmt, parseTree));
        break;
    case T_AlterRoleStmt:
        HandleAlterRole(castNode(AlterRoleStmt, parseTree));
        break;
    case T_DropRoleStmt:
        HandleDropRole(castNode(DropRoleStmt, parseTree));
        break;
    default:
        break;
    }

    if(PreviousProcessUtilityHook)
    {
        PreviousProcessUtilityHook(
            pstmt,
            queryString,
            readOnlyTree,
            context,
            params,
            queryEnv,
            dest,
            qc);
    }
    else
    {
        standard_ProcessUtility(
            pstmt,
            queryString,
            readOnlyTree,
            context,
            params,
            queryEnv,
            dest,
            qc);
    }
}

void InitConsoleConnector()
{
    PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = NeonProcessUtility;
    RegisterXactCallback(NeonXactCallback, NULL);
    RegisterSubXactCallback(NeonSubXactCallback, NULL);
    CurlHandle = curl_easy_init();
    if(!CurlHandle)
    {
        errmsg("Failed to initialize curl");
    }
}

void CleanupConsoleConnector()
{
    curl_easy_cleanup(CurlHandle);
}
