/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*-------------------------------------------------------------------------
 *
 * identity.c
 *	  Provides the process identity management. One of the most important usage
 *	  of identity is to support unique tag. And there are two identity, one is
 *	  static one used to locate server(phasical segment). The other one is used
 *	  to run the work(query).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cdb/identity.h"

#include "lib/stringinfo.h"				/* serialize */
#include "miscadmin.h"					/* IsBootstrapProcessingMode() */
#include "executor/execdesc.h"			/* AllocateResource */
#include "optimizer/cost.h"
#include "utils/guc.h"

#include "commands/defrem.h"			/* defGetInt64() */

int slaveHostNumber;

typedef enum SegmentRole
{
	SEGMENT_ROLE_INVALID,
	SEGMENT_ROLE_INITDB,
	SEGMENT_ROLE_MASTER,
	SEGMENT_ROLE_STANDBY,
	SEGMENT_ROLE_SEGMENT,
	SEGMENT_ROLE_GTM,
	SEGMENT_ROLE_CATALOGSERVICE,
	SEGMENT_ROLE_STANDALONE
} SegmentRole;

typedef struct SegmentIdentity
{
	SegmentRole	role;

	/*
	 * There are two level identitiers for each process. One is Segment name,
	 * which is used to locate the physical server, another one is Query
	 * executor id, used to track the query workers.
	 *
	 * Process 'ps' state and log should output all of them.
	 *
	 * Segment name is default to 'role + hostname'.
	 */
	char		name[SEGMENT_IDENTITY_NAME_LENGTH];

	/* Allocate during register. */
	int			id;

	/* Cache our self information. */
	bool		master_address_set;
	char		master_host[SEGMENT_IDENTITY_NAME_LENGTH];
	int			master_port;

	SegmentFunctionList	function;
	ProcessIdentity		pid;
} SegmentIdentity;

static SegmentIdentity SegmentId;

static void DebugSegmentIdentity(struct SegmentIdentity *id);
static void DebugProcessIdentity(struct ProcessIdentity *id);
static bool	DeserializeProcessIdentity(struct ProcessIdentity *id, const char *str);

static void
SetSegmentRole(const char *name, SegmentIdentity *segment)
{
	SegmentRole	role = SEGMENT_ROLE_INVALID;

	if (IsBootstrapProcessingMode())
		role = SEGMENT_ROLE_INITDB;
	else if (strcmp("segment", name) == 0)
		role =  SEGMENT_ROLE_SEGMENT;
	else if (strcmp("master", name) == 0)
		role = SEGMENT_ROLE_MASTER;
	else if (strcmp("standby", name) == 0)
		role = SEGMENT_ROLE_STANDBY;
	else if (strcmp("gtm", name) == 0)
		role = SEGMENT_ROLE_GTM;
	else if (strcmp("catalogservice", name) == 0)
		role = SEGMENT_ROLE_CATALOGSERVICE;
	else
		elog(FATAL, "Invalid role: %s!", name);

	segment->role = role;
}

static void
SetupSegmentFunction(SegmentIdentity *segment)
{
	Assert(segment);
	MemSet(&segment->function, 0, sizeof(segment->function));

	switch (segment->role)
	{
		case SEGMENT_ROLE_INITDB:
			break;
		case SEGMENT_ROLE_MASTER:
			segment->function.module_motion = true;
			break;
		case SEGMENT_ROLE_STANDBY:
			segment->function.module_log_sync = true;
			break;
		case SEGMENT_ROLE_SEGMENT:
			segment->function.login_as_default = true;
			segment->function.module_motion = true;
			break;
		default:
			Assert(false);
			break;
	}
}

static void
SetupSegmentName(SegmentIdentity *segment)
{
}

static void
UnsetProcessIdentity(SegmentIdentity *segment)
{
	segment->pid.init = false;
}

void
SetSegmentIdentity(const char *name)
{
	SetSegmentRole(name, &SegmentId);
	SetupSegmentName(&SegmentId);
	SetupSegmentFunction(&SegmentId);
	UnsetProcessIdentity(&SegmentId);
}

bool
IsOnMaster(void)
{
	return SegmentId.role == SEGMENT_ROLE_MASTER;
}

static void
GenerateProcessIdentityLabel(ProcessIdentity *id)
{
	Assert(id->init);
}

#define	PI_SER_START_TOKEN	"ProcessIdentity_Begin_"
#define PI_SER_SLICE_TOKEN	"slice_"
#define PI_SER_IDX_TOKEN	"idx_"
#define	PI_SER_GANG_TOKEN	"gang_"
#define PI_SER_WRITER_TOKEN	"writer_"
#define PI_SER_CMD_TOKEN	"cmd_"
#define PI_SER_END_TOKEN	"End_ProcessIdentity"

const char *
SerializeProcessIdentity(ProcessIdentity *id, int *msg_len)
{
	StringInfoData	str;

#define put_token_int(token, val) \
do { \
	appendStringInfo(&str, token "%d" "_", (val)); \
} while (0)

#define put_token_bool(token, val) \
do { \
	appendStringInfo(&str, token "%s" "_", (val) ? "t" : "f"); \
} while (0)
	
	/* Should not happen, but return NULL instead of error! */
	if (!id->init)
		return NULL;

	/* Prepare to serialize */
	initStringInfo(&str);
	appendStringInfo(&str, PI_SER_START_TOKEN);

	/* serialize the data from here */
	put_token_int(PI_SER_SLICE_TOKEN, id->slice_id);
	put_token_int(PI_SER_IDX_TOKEN,  id->id_in_slice);
	put_token_int(PI_SER_GANG_TOKEN, id->gang_member_num);
	put_token_int(PI_SER_CMD_TOKEN, id->command_count);
	put_token_bool(PI_SER_WRITER_TOKEN, id->is_writer);

	/* End of serialize */
	appendStringInfo(&str, PI_SER_END_TOKEN);

	*msg_len = str.len;
	return str.data;
}

static bool
DeserializeProcessIdentity(ProcessIdentity *id, const char *str)
{
	const char	*p;

	Assert(id);
	Assert(str);

	id->init = false;
	p = str;

#define consume_token(token)	\
do { \
	if (strncmp(p, (token), strlen(token)) != 0) \
		goto error; \
	p += strlen(token); \
} while (0)

#define consume_int(val) \
do { \
	char *end; \
	(val) = strtol(p, &end, 10); \
	if (p == end) \
		goto error; \
	p = end; \
	if (*p != '_') \
		goto error; \
	p++; /* skip the '_' */ \
} while (0)

#define consume_bool(val) \
do { \
	if (*p == 't') \
		(val) = true; \
	else if (*p == 'f') \
		(val) = false; \
	else \
		goto error; \
	p++; \
	if (*p != '_') \
		goto error; \
	p++; \
} while (0)

	consume_token(PI_SER_START_TOKEN);
	consume_token(PI_SER_SLICE_TOKEN);
	consume_int(id->slice_id);
	consume_token(PI_SER_IDX_TOKEN);
	consume_int(id->id_in_slice);
	consume_token(PI_SER_GANG_TOKEN);
	consume_int(id->gang_member_num);
	consume_token(PI_SER_CMD_TOKEN);
	consume_int(id->command_count);
	consume_token(PI_SER_WRITER_TOKEN);
	consume_bool(id->is_writer);
	consume_token(PI_SER_END_TOKEN);

	return true;

error:
	return false;
}

bool
SetupProcessIdentity(const char *str)
{
	bool ret = false;

	ret = DeserializeProcessIdentity(&SegmentId.pid, str);

	DebugSegmentIdentity(&SegmentId);
	DebugProcessIdentity(&SegmentId.pid);

	SegmentId.pid.init = true;
	GenerateProcessIdentityLabel(&SegmentId.pid);

	return ret;
}

bool
AmIMaster(void)
{
	return SegmentId.role == SEGMENT_ROLE_MASTER;
}

bool
AmIStandby(void)
{
	return SegmentId.role == SEGMENT_ROLE_STANDBY;
}

bool
AmISegment(void)
{
	return SegmentId.role == SEGMENT_ROLE_SEGMENT;
}

bool
AmIGtm(void)
{
	return SegmentId.role == SEGMENT_ROLE_GTM;
}

bool
AmICatalogService(void)
{
	return SegmentId.role == SEGMENT_ROLE_CATALOGSERVICE;
}

static void
DebugSegmentIdentity(SegmentIdentity *id)
{
}

static void
DebugProcessIdentity(ProcessIdentity *id)
{
	if (!id->init)
	{
		elog(DEBUG1, "ProcessIdentity is not init");
	}

	elog(DEBUG1, "ProcessIdentity: "
				"slice %d "
				"id %d "
				"gang num %d "
				"writer %s",
				id->slice_id,
				id->id_in_slice,
				id->gang_member_num,
				id->is_writer ? "t" : "f");
}
