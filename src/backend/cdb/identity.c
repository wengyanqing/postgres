#include "postgres.h"
#include "miscadmin.h"          // IsBootstrapProcessingMode

#include "cdb/identity.h"

static CdbNodeIdentity NodeId = { CDB_NODE_ROLE_INVALID };

static void
SetCdbNodeRole(const char *name, CdbNodeIdentity *node)
{
   	CdbNodeRole role = CDB_NODE_ROLE_INVALID;

    if (IsBootstrapProcessingMode())
        role = CDB_NODE_ROLE_INITDB;
    else if (strcmp("master", name) == 0)
        role = CDB_NODE_ROLE_MASTER;
    else if (strcmp("segment", name) == 0)
        role = CDB_NODE_ROLE_SEGMENT;
    else if (strcmp("gtm", name) == 0)
        role = CDB_NODE_ROLE_GTM;
    else if (strcmp("catalogservice", name) == 0)
        role = CDB_NODE_ROLE_CATALOGSERVICE;
    else
        elog(FATAL, "Invalid role: %s!", name);

    node->role = role;
}

static void
SetupCdbNodeName(CdbNodeIdentity *node)
{
}

static void
UnsetProcessIdentity(CdbNodeIdentity *node)
{
    node->pid.init = false;
}

void
SetCdbNodeIdentity(const char *name)
{
    SetCdbNodeRole(name, &NodeId);
    SetupCdbNodeName(&NodeId);
    //SetupSegmentFunction(&SegmentId);
    UnsetProcessIdentity(&NodeId);
}

#define IS_MASTER_NODE() (NodeId->role == CDB_NODE_ROLE_MASTER)
#define IS_SEGMENT_NODE() (NodeId->role == CDB_NODE_ROLE_SEGMENT)
#define IS_GTM_NODE() (NodeId->role == CDB_NODE_ROLE_GTM)
#define IS_CATALOGSERVICE_NODE() (NodeId->role == CDB_NODE_ROLE_CATALOGSERVICE)
