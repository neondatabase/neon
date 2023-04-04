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
#include <curl/curl.h>

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static char *ConsoleURL = NULL;

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

// Includes null terminator
static size_t ComputeLengthOfDeltas()
{
    size_t length = sizeof('[');
    if(RootTable.db_table)
    {
        HASH_SEQ_STATUS status;
        DbEntry *entry;
        hash_seq_init(&status, RootTable.db_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            length += sizeof('{');
            length += strlen("\"op\":");
            length += sizeof('"');
            length += entry->type == Op_Set ? strlen("set") : strlen("del");
            length += sizeof('"');
            length += strlen(", \"name\":");
            length += sizeof('"');
            length += strlen(entry->name);
            length += sizeof('"');
            length += strlen(", \"type\":\"db\"");
            if(entry->owner[0] != '\0')
            {
                length += strlen(", \"owner\":");
                length += sizeof('"');
                length += strlen(entry->owner);
                length += sizeof('"');
            }
            if(entry->old_name[0] != '\0')
            {
                length += strlen(", \"old_name\":");
                length += sizeof('"');
                length += strlen(entry->old_name);
                length += sizeof('"');
            }
            length += sizeof('}');
            length += sizeof(',');
        }
    }

    if(RootTable.role_table)
    {
        HASH_SEQ_STATUS status;
        RoleEntry *entry;
        hash_seq_init(&status, RootTable.role_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            length += sizeof('{');
            length += strlen("\"op\":");
            length += sizeof('"');
            length += entry->type == Op_Set ? strlen("set") : strlen("del");
            length += sizeof('"');
            length += strlen(", \"name\":");
            length += sizeof('"');
            length += strlen(entry->name);
            length += sizeof('"');
            length += strlen(", \"type\":\"role\"");
            if(entry->password)
            {
                length += strlen(", \"password\":");
                length += sizeof('"');
                length += strlen(entry->password);
                length += sizeof('"');
            }
            if(entry->old_name[0] != '\0')
            {
                length += strlen(", \"old_name\":");
                length += sizeof('"');
                length += strlen(entry->old_name);
                length += sizeof('"');
            }
            length += sizeof('}');
            length += sizeof(',');
        }
    }
    // We have added an extra byte due to the trailing comma. However,
    // we also need room for null terminator, so we don't touch it.
    length += sizeof(']');
    return length;
}

void AppendChar(char **message, char c)
{
    **message = c;
    (*message) += 1;
}

void AppendString(char **message, const char *str)
{
    size_t length = strlen(str);
    memcpy(*message, str, length);
    (*message) += length;
}

void ConstructDeltaMessage(char *message)
{
#define AppendChar(x) AppendChar(&message, x)
#define AppendString(x) AppendString(&message, x)

    AppendChar('[');
    if(RootTable.db_table)
    {
        HASH_SEQ_STATUS status;
        DbEntry *entry;
        hash_seq_init(&status, RootTable.db_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            AppendChar('{');
            AppendString("\"op\":");
            AppendChar('"');
            if(entry->type == Op_Set)
                AppendString("set");
            else
                AppendString("del");
            AppendChar('"');
            AppendString(", \"name\":");
            AppendChar('"');
            AppendString(entry->name);
            AppendChar('"');
            AppendString(", \"type\":\"db\"");
            if(entry->owner[0] != '\0')
            {
                AppendString(", \"owner\":");
                AppendChar('"');
                AppendString(entry->owner);
                AppendChar('"');
            }
            if(entry->old_name[0] != '\0')
            {
                AppendString(", \"old_name\":");
                AppendChar('"');
                AppendString(entry->old_name);
                AppendChar('"');
            }
            AppendChar('}');
            AppendChar(',');
        }
    }

    if(RootTable.role_table)
    {
        HASH_SEQ_STATUS status;
        RoleEntry *entry;
        hash_seq_init(&status, RootTable.role_table);
        while((entry = hash_seq_search(&status)) != NULL)
        {
            AppendChar('{');
            AppendString("\"op\":");
            AppendChar('"');
            if(entry->type == Op_Set)
                AppendString("set");
            else
                AppendString("del");
            AppendChar('"');
            AppendString(", \"name\":");
            AppendChar('"');
            AppendString(entry->name);
            AppendChar('"');
            AppendString(", \"type\":\"role\"");
            if(entry->password)
            {
                AppendString(", \"password\":");
                AppendChar('"');
                AppendString(entry->password);
                AppendChar('"');
            }
            if(entry->old_name[0] != '\0')
            {
                AppendString(", \"old_name\":");
                AppendChar('"');
                AppendString(entry->old_name);
                AppendChar('"');
            }
            AppendChar('}');
            AppendChar(',');
        }
    }
    *(message - 1) = ']';
    AppendChar('\0');
#undef AppendChar
#undef AppendString
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
    CURL *curl = curl_easy_init();
    if(!curl)
    {
        elog(ERROR, "Failed to initialize curl object");
        return;
    }

    size_t length_of_message = ComputeLengthOfDeltas(); // Includes null terminator
    char *message = palloc(length_of_message);
    ConstructDeltaMessage(message);

    struct curl_slist *header = NULL;
    header = curl_slist_append(header, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, message);
    curl_easy_setopt(curl, CURLOPT_URL, ConsoleURL);
    if(curl_easy_perform(curl) != 0)
    {
        elog(ERROR, "Failed to perform curl request");
    }
    curl_easy_cleanup(curl);
    curl_slist_free_all(header);
    pfree(message);
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

    if(old_table->role_table)
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
        entry->old_name[0] = '\0';

    entry->type = Op_Set;
    if(downer)
        strncpy(entry->owner, defGetString(downer), NAMEDATALEN);
    else
        strncpy(entry->owner, GetUserNameFromId(GetUserId(), false), NAMEDATALEN);
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
        entry->old_name[0] = '\0';

    strncpy(entry->owner, get_rolespec_name(stmt->newowner), NAMEDATALEN);
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
            strncpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
        else
            strncpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        strncpy(entry_for_new_name->owner, entry->owner, NAMEDATALEN);
        hash_search(
            CurrentDdlTable->db_table,
            stmt->subname,
            HASH_REMOVE,
            NULL);
    }
    else
    {
        strncpy(entry_for_new_name->old_name, stmt->subname, NAMEDATALEN);
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
    entry->owner[0] = '\0';
    if(!found)
        entry->old_name[0] = '\0';
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
        entry->old_name[0] = '\0';
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
        entry->old_name[0] = '\0';
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
            strncpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
        else
            strncpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
        entry_for_new_name->password = entry->password;
        hash_search(
            CurrentDdlTable->role_table,
            entry->name,
            HASH_REMOVE,
            NULL);
    }
    else
    {
        strncpy(entry_for_new_name->old_name, stmt->subname, NAMEDATALEN);
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
            entry->old_name[0] = '\0';
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
}
