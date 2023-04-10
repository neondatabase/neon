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
#include "utils/guc.h"
#include "port.h"
#include <curl/curl.h>
#include "utils/jsonb.h"

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static char *ConsoleURL = NULL;
static CURL *CurlHandle;
static struct curl_slist *ContentHeader = NULL;

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
    HTAB *db_table;
    HTAB *role_table;
} DdlHashTable;

static DdlHashTable RootTable;
static DdlHashTable *CurrentDdlTable = &RootTable;

static void PushKeyValue(JsonbParseState **state, char *key, char *value)
{
    JsonbValue k, v;
    k.type = jbvString;
    k.val.string.len = strlen(key);
    k.val.string.val = key;
    v.type = jbvString;
    v.val.string.len = strlen(value);
    v.val.string.val = value;
    pushJsonbValue(state, WJB_KEY, &k);
    pushJsonbValue(state, WJB_VALUE, &v);
}

static char *ConstructDeltaMessage()
{
    JsonbParseState *state = NULL;
    pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
    if(RootTable.db_table)
    {
        HASH_SEQ_STATUS status;
        DbEntry *entry;
        hash_seq_init(&status, RootTable.db_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
            PushKeyValue(&state, "op", entry->type == Op_Set ? "set" : "del");
            PushKeyValue(&state, "name", entry->name);
            PushKeyValue(&state, "type", "db");
            if(entry->owner[0] != '\0')
            {
                PushKeyValue(&state, "owner", entry->owner);
            }
            if(entry->old_name[0] != '\0')
            {
                PushKeyValue(&state, "old_name", entry->old_name);
            }
            pushJsonbValue(&state, WJB_END_OBJECT, NULL);
        }
    }

    if(RootTable.role_table)
    {
        HASH_SEQ_STATUS status;
        RoleEntry *entry;
        hash_seq_init(&status, RootTable.role_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
            PushKeyValue(&state, "op", entry->type == Op_Set ? "set" : "del");
            PushKeyValue(&state, "name", entry->name);
            PushKeyValue(&state, "type", "role");
            if(entry->password)
            {
                PushKeyValue(&state, "password", (char *)entry->password);
            }
            if(entry->old_name[0] != '\0')
            {
                PushKeyValue(&state, "old_name", entry->old_name);
            }
            pushJsonbValue(&state, WJB_END_OBJECT, NULL);
        }
    }
    JsonbValue *result = pushJsonbValue(&state, WJB_END_ARRAY, NULL);
    Jsonb *jsonb = JsonbValueToJsonb(result);
    return JsonbToCString(NULL, &jsonb->root, 0 /*estimated_len*/);
}

static void SendDeltasToConsole()
{
    if(!RootTable.db_table && !RootTable.role_table)
        return;
    if(!ConsoleURL)
    {
        elog(LOG, "ConsoleURL not set, skipping forwarding");
        return;
    }

    char *message = ConstructDeltaMessage();

    curl_easy_setopt(CurlHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
    curl_easy_setopt(CurlHandle, CURLOPT_HTTPHEADER, ContentHeader);
    curl_easy_setopt(CurlHandle, CURLOPT_POSTFIELDS, message);
    curl_easy_setopt(CurlHandle, CURLOPT_URL, ConsoleURL);

    const int num_retries = 5;
    int curl_status;
    for(int i = 0; i < num_retries; i++)
    {
        if((curl_status = curl_easy_perform(CurlHandle)) == 0)
            break;
        pg_usleep(1000 * 1000);
    }
    if(curl_status != 0)
        elog(ERROR, "Failed to perform curl request");
}

static void InitDbTableIfNeeded()
{
    if(!CurrentDdlTable->db_table)
    {
        HASHCTL db_ctl = {};
        db_ctl.keysize = NAMEDATALEN;
        db_ctl.entrysize = sizeof(DbEntry);
        db_ctl.hcxt = CurTransactionContext;
        CurrentDdlTable->db_table = hash_create(
            "Dbs Created",
            4,
            &db_ctl,
            HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
    }
}

static void InitRoleTableIfNeeded()
{
    if(!CurrentDdlTable->role_table)
    {
        HASHCTL role_ctl = {};
        role_ctl.keysize = NAMEDATALEN;
        role_ctl.entrysize = sizeof(RoleEntry);
        role_ctl.hcxt = CurTransactionContext;
        CurrentDdlTable->role_table = hash_create(
            "Roles Created",
            4,
            &role_ctl,
            HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
    }
}

static void PushTable()
{
    DdlHashTable *new_table = MemoryContextAlloc(CurTransactionContext, sizeof(DdlHashTable));
    new_table->prev_table = CurrentDdlTable;
    new_table->role_table = NULL;
    new_table->db_table = NULL;
    CurrentDdlTable = new_table;
}

static void MergeTable()
{
    DdlHashTable *old_table = CurrentDdlTable;
    CurrentDdlTable = old_table->prev_table;

    if(old_table->db_table)
    {
        InitDbTableIfNeeded();
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
                strlcpy(to_write->owner, entry->owner, NAMEDATALEN);
            strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
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
                        strlcpy(to_write->old_name, old->old_name, NAMEDATALEN);
                    else
                        strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
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

    if(old_table->role_table)
    {
        InitRoleTableIfNeeded();
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
            strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
            if(entry->old_name[0] != '\0')
            {
                bool found_old = false;
                RoleEntry *old = hash_search(
                    CurrentDdlTable->role_table,
                    entry->old_name,
                    HASH_FIND,
                    &found_old);
                if(found_old)
                {
                    if(old->old_name[0] != '\0')
                        strlcpy(to_write->old_name, old->old_name, NAMEDATALEN);
                    else
                        strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
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
    RootTable.role_table = NULL;
    RootTable.db_table = NULL;
    Assert(CurrentDdlTable == &RootTable);
}

static void HandleCreateDb(CreatedbStmt *stmt)
{
    InitDbTableIfNeeded();
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
    if(!found)
        memset(entry->old_name, 0, sizeof(entry->old_name));

    entry->type = Op_Set;
    if(downer)
        strlcpy(entry->owner, defGetString(downer), NAMEDATALEN);
    else
        strlcpy(entry->owner, GetUserNameFromId(GetUserId(), false), NAMEDATALEN);
}

static void HandleAlterOwner(AlterOwnerStmt *stmt)
{
    InitDbTableIfNeeded();
    if(stmt->objectType != OBJECT_DATABASE)
        return;
    const char *name = strVal(stmt->object);
    bool found = false;
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        name,
        HASH_ENTER,
        &found);
    if(!found)
        memset(entry->old_name, 0, sizeof(entry->old_name));

    strlcpy(entry->owner, get_rolespec_name(stmt->newowner), NAMEDATALEN);
    entry->type = Op_Set;
}

static void HandleDbRename(RenameStmt *stmt)
{
    Assert(stmt->renameType == OBJECT_DATABASE);
    InitDbTableIfNeeded();
    bool found = false;
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        stmt->subname,
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
            strlcpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
        else
            strlcpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        strlcpy(entry_for_new_name->owner, entry->owner, NAMEDATALEN);
        hash_search(
            CurrentDdlTable->db_table,
            stmt->subname,
            HASH_REMOVE,
            NULL);
    }
    else
    {
        strlcpy(entry_for_new_name->old_name, stmt->subname, NAMEDATALEN);
        entry_for_new_name->owner[0] = '\0';
    }
}

static void HandleDropDb(DropdbStmt *stmt)
{
    InitDbTableIfNeeded();
    bool found = false;
    DbEntry *entry = hash_search(
        CurrentDdlTable->db_table,
        stmt->dbname,
        HASH_ENTER,
        &found);
    entry->type = Op_Delete;
    memset(entry->owner, 0, sizeof(entry->owner));
    if(!found)
        memset(entry->old_name, 0, sizeof(entry->owner));

}

static void HandleCreateRole(CreateRoleStmt *stmt)
{
    InitRoleTableIfNeeded();
    bool found = false;
    RoleEntry *entry = hash_search(
        CurrentDdlTable->role_table,
        stmt->role,
        HASH_ENTER,
        &found);
    DefElem *dpass = NULL;
    ListCell *option;
    foreach(option, stmt->options)
    {
        DefElem *defel = lfirst(option);
        if(strcmp(defel->defname, "password") == 0)
            dpass = defel;
    }
    if(!found)
        memset(entry->old_name, 0, sizeof(entry->old_name));
    if(dpass)
        entry->password = MemoryContextStrdup(CurTransactionContext, strVal(dpass->arg));
    else
        entry->password = NULL;
    entry->type = Op_Set;
}

static void HandleAlterRole(AlterRoleStmt *stmt)
{
    InitRoleTableIfNeeded();
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
    bool found = false;
    RoleEntry *entry = hash_search(
        CurrentDdlTable->role_table,
        stmt->role->rolename,
        HASH_ENTER,
        &found);
    if(!found)
        memset(entry->old_name, 0, sizeof(entry->old_name));
    entry->password = MemoryContextStrdup(CurTransactionContext, strVal(dpass->arg));
    entry->type = Op_Set;
}

static void HandleRoleRename(RenameStmt *stmt)
{
    InitRoleTableIfNeeded();
    Assert(stmt->renameType == OBJECT_ROLE);
    bool found = false;
    RoleEntry *entry = hash_search(
        CurrentDdlTable->role_table,
        stmt->subname,
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
            strlcpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
        else
            strlcpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        entry_for_new_name->password = entry->password;
        hash_search(
            CurrentDdlTable->role_table,
            entry->name,
            HASH_REMOVE,
            NULL);
    }
    else
    {
        strlcpy(entry_for_new_name->old_name, stmt->subname, NAMEDATALEN);
        entry_for_new_name->password = NULL;
    }
}

static void HandleDropRole(DropRoleStmt *stmt)
{
    InitRoleTableIfNeeded();
    ListCell *item;
    foreach(item, stmt->roles)
    {
        RoleSpec *spec = lfirst(item);
        bool found = false;
        RoleEntry *entry = hash_search(
            CurrentDdlTable->role_table,
            spec->rolename,
            HASH_ENTER,
            &found);
        entry->type = Op_Delete;
        entry->password = NULL;
        if(!found)
            memset(entry->old_name, 0, sizeof(entry));
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

    DefineCustomStringVariable(
        "neon.console_url",
        "URL of the Neon Console, which will be forwarded changes to dbs and roles",
        NULL,
        &ConsoleURL,
        "",
        PGC_SUSET,
        0,
        NULL,
        NULL,
        NULL);

    if(curl_global_init(CURL_GLOBAL_DEFAULT))
    {
        elog(ERROR, "Failed to initialize curl");
    }
    if((CurlHandle = curl_easy_init()) == NULL)
    {
        elog(ERROR, "Failed to initialize curl handle");
    }
    if((ContentHeader = curl_slist_append(ContentHeader, "Content-Type: application/json")) == NULL)
    {
        elog(ERROR, "Failed to initialize content header");
    }
}
